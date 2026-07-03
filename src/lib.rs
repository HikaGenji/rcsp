//! rcsp engine — a discrete-event, rank-ordered stream-processing graph engine.
//!
//! This is the Rust backend for `rcsp`, a clone of Point72's CSP. It mirrors
//! CSP's own architecture: the engine (this crate) is compiled, while the
//! computation nodes are written in Python and driven by the engine.
//!
//! ## Model
//! * The graph is a DAG of **nodes** connected by typed time-series **edges**.
//! * Execution is a discrete-event simulation. A priority queue orders work by
//!   `(time, seq)`. Each distinct timestamp is an **engine cycle**.
//! * Within a cycle nodes fire in **topological rank order**, so a node always
//!   observes every upstream tick made at the same timestamp before it runs.
//! * Every input tracks `valid` (has ever ticked) and `ticked` (ticked this
//!   cycle). A node fires only when at least one input ticks in the cycle.
//! * Nodes may emit onto output edges (propagates within the current cycle) and
//!   may schedule **alarms** — future injections onto their own edges (timers,
//!   delays, user alarms). This is how time enters the graph.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyTuple};

/// A value flowing on an edge. Native kernels use the typed variants; Python
/// nodes round-trip arbitrary objects through `Py`.
enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Py(Py<PyAny>),
}

impl Clone for Value {
    fn clone(&self) -> Self {
        match self {
            Value::Int(i) => Value::Int(*i),
            Value::Float(f) => Value::Float(*f),
            Value::Bool(b) => Value::Bool(*b),
            Value::Str(s) => Value::Str(s.clone()),
            // The engine only clones values while the GIL is held (during run);
            // re-entrant `with_gil` just hands back the existing token.
            Value::Py(o) => Python::with_gil(|py| Value::Py(o.clone_ref(py))),
        }
    }
}

impl Value {
    fn from_py(obj: &Bound<'_, PyAny>) -> Value {
        // bool must be checked before int: Python bool is a subclass of int.
        if let Ok(b) = obj.downcast::<PyBool>() {
            return Value::Bool(b.is_true());
        }
        if let Ok(i) = obj.extract::<i64>() {
            return Value::Int(i);
        }
        if let Ok(f) = obj.extract::<f64>() {
            return Value::Float(f);
        }
        if let Ok(s) = obj.extract::<String>() {
            return Value::Str(s);
        }
        Value::Py(obj.clone().unbind())
    }

    fn to_py(&self, py: Python<'_>) -> PyObject {
        match self {
            Value::Int(i) => i.into_py(py),
            Value::Float(f) => f.into_py(py),
            Value::Bool(b) => b.into_py(py),
            Value::Str(s) => s.into_py(py),
            Value::Py(o) => o.clone_ref(py),
        }
    }

    /// Best-effort numeric view for native arithmetic kernels.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Py(_) => true,
        }
    }
}

/// The native kernels. Anything not expressible here is a `Python` node.
enum Kernel {
    Const { value: Value },
    Timer { interval: i64, value: Value },
    Delay { delta: i64 },
    Count,
    FirstN { n: i64 },
    BinOp { op: BinOp },
    Filter,
    Sample,
    Merge,
    Print { name: String },
    GraphOutput { name: String },
    Python { func: Py<PyAny>, run_at_start: bool },
}

#[derive(Clone, Copy)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

struct Node {
    name: String,
    inputs: Vec<usize>,  // edge ids, incl. alarm edges (which have no producer)
    outputs: Vec<usize>, // edge ids
    /// For Python nodes: index into `inputs` where alarm edges begin.
    alarm_base: usize,
    kernel: Kernel,
    rank: i64,
}

#[derive(Default, Clone)]
struct EdgeState {
    value: Option<Value>,
    last_tick: Option<i64>, // id of the cycle in which this edge last ticked
    producer: Option<usize>,
}

/// Queued future work: either inject a value onto an edge, or run a node.
enum Sched {
    Edge(usize, Value),
    Node(usize),
}

#[pyclass]
pub struct Engine {
    nodes: Vec<Node>,
    edges: Vec<EdgeState>,
    consumers: Vec<Vec<usize>>, // edge id -> node ids consuming it
    counters: Vec<i64>,         // per-node scratch (Count/FirstN)
    outputs: HashMap<String, Vec<(i64, PyObject)>>,
    push_adapters: Vec<(usize, Py<PyAny>)>, // (edge, python queue) for realtime push
    cycle: i64,                             // monotonic engine-cycle counter
    profile: bool,                          // collect per-node timing this run
    prof_count: Vec<u64>,                   // per-node execution count
    prof_ns: Vec<u128>,                     // per-node cumulative exec time (ns)
    endtime: i64,
    // Stepped-execution state (for dynamic graphs): the scheduler persists
    // across step() calls so Python can add sub-graphs between steps.
    s_timed: BinaryHeap<Reverse<(i64, u64)>>,
    s_payload: HashMap<u64, Sched>,
    s_seq: u64,
    s_end: i64,
    s_now: i64,
    s_integrated: usize, // nodes already ranked/wired/seeded
}

#[pymethods]
impl Engine {
    #[new]
    fn new() -> Self {
        Engine {
            nodes: Vec::new(),
            edges: Vec::new(),
            consumers: Vec::new(),
            counters: Vec::new(),
            outputs: HashMap::new(),
            push_adapters: Vec::new(),
            cycle: 0,
            profile: false,
            prof_count: Vec::new(),
            prof_ns: Vec::new(),
            endtime: 0,
            s_timed: BinaryHeap::new(),
            s_payload: HashMap::new(),
            s_seq: 0,
            s_end: 0,
            s_now: 0,
            s_integrated: 0,
        }
    }

    /// Begin a stepped run (used for dynamic graphs). Seeds sources and holds
    /// the scheduler as engine state; drive it with repeated `step()` calls and
    /// read results with `outputs()`.
    #[pyo3(signature = (start_ns, end_ns, profile = false))]
    fn begin(&mut self, start_ns: i64, end_ns: i64, profile: bool) -> PyResult<()> {
        self.endtime = end_ns;
        self.cycle = 0;
        self.profile = profile;
        let n = self.nodes.len();
        self.prof_count = vec![0; n];
        self.prof_ns = vec![0; n];
        self.finalize()?;
        self.rebuild_consumers();
        self.s_timed = BinaryHeap::new();
        self.s_payload = HashMap::new();
        self.s_seq = 0;
        self.s_end = end_ns;
        self.s_now = start_ns;
        for v in self.outputs.values_mut() {
            v.clear();
        }
        for nid in 0..n {
            self.seed_source(nid, start_ns);
        }
        self.s_integrated = n;
        Ok(())
    }

    /// Advance one timestamp. Returns the processed time, or `None` when done.
    /// Before advancing, integrates any nodes added since the last step.
    fn step(&mut self, py: Python<'_>) -> PyResult<Option<i64>> {
        self.integrate()?;

        let mut timed = std::mem::take(&mut self.s_timed);
        let mut payload = std::mem::take(&mut self.s_payload);
        let mut seq = self.s_seq;

        let result = (|engine: &mut Engine| -> PyResult<Option<i64>> {
            let now = match timed.peek().copied() {
                Some(Reverse((t, _))) if t <= engine.s_end => t,
                _ => return Ok(None),
            };
            engine.s_now = now;
            // Drain all cycles at `now` (feedback may add follow-up cycles).
            while timed.peek().map_or(false, |Reverse((t, _))| *t == now) {
                engine.process_cycle(py, now, engine.s_end, &mut timed, &mut payload, &mut seq)?;
            }
            Ok(Some(now))
        })(self);

        self.s_timed = timed;
        self.s_payload = payload;
        self.s_seq = seq;
        result
    }

    /// Return the collected graph outputs from a stepped run.
    fn outputs(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect_outputs(py)
    }

    /// Per-node profiling from the last run: list of
    /// `(id, name, exec_count, total_ns)` for nodes that executed.
    fn profiling_report(&self, py: Python<'_>) -> PyResult<PyObject> {
        let rows = PyList::empty_bound(py);
        for (nid, node) in self.nodes.iter().enumerate() {
            let count = self.prof_count.get(nid).copied().unwrap_or(0);
            if count == 0 {
                continue;
            }
            let total = self.prof_ns.get(nid).copied().unwrap_or(0);
            let tup = PyTuple::new_bound(
                py,
                &[
                    nid.into_py(py),
                    node.name.clone().into_py(py),
                    count.into_py(py),
                    (total as u64).into_py(py),
                ],
            );
            rows.append(tup)?;
        }
        Ok(rows.into())
    }

    /// Register a realtime push adapter: items placed on `queue` (a Python
    /// `queue.Queue`) are injected onto `edge` at wall-clock arrival time.
    fn register_push_adapter(&mut self, edge: usize, queue: Py<PyAny>) {
        self.push_adapters.push((edge, queue));
    }

    /// Allocate a fresh edge and return its id.
    fn new_edge(&mut self) -> usize {
        self.edges.push(EdgeState::default());
        self.consumers.push(Vec::new());
        self.edges.len() - 1
    }

    fn add_const(&mut self, out: usize, value: Bound<'_, PyAny>) -> usize {
        let alarm = self.new_edge();
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "const".into(),
            inputs: vec![alarm],
            outputs: vec![out],
            alarm_base: 0,
            kernel: Kernel::Const { value: Value::from_py(&value) },
            rank: 0,
        })
    }

    fn add_timer(&mut self, out: usize, interval_ns: i64, value: Bound<'_, PyAny>) -> usize {
        let alarm = self.new_edge();
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "timer".into(),
            inputs: vec![alarm],
            outputs: vec![out],
            alarm_base: 0,
            kernel: Kernel::Timer { interval: interval_ns, value: Value::from_py(&value) },
            rank: 0,
        })
    }

    fn add_delay(&mut self, x: usize, out: usize, delta_ns: i64) -> usize {
        let alarm = self.new_edge();
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "delay".into(),
            inputs: vec![x, alarm],
            outputs: vec![out],
            alarm_base: 1,
            kernel: Kernel::Delay { delta: delta_ns },
            rank: 0,
        })
    }

    fn add_binop(&mut self, op: &str, a: usize, b: usize, out: usize) -> PyResult<usize> {
        let sym = match op {
            "add" => "+",
            "sub" => "-",
            "mul" => "*",
            "div" => "/",
            "gt" => ">",
            "lt" => "<",
            "ge" => ">=",
            "le" => "<=",
            "eq" => "==",
            "ne" => "!=",
            _ => return Err(pyo3::exceptions::PyValueError::new_err(format!("unknown op {op}"))),
        };
        let op = match op {
            "add" => BinOp::Add,
            "sub" => BinOp::Sub,
            "mul" => BinOp::Mul,
            "div" => BinOp::Div,
            "gt" => BinOp::Gt,
            "lt" => BinOp::Lt,
            "ge" => BinOp::Ge,
            "le" => BinOp::Le,
            "eq" => BinOp::Eq,
            "ne" => BinOp::Ne,
            _ => unreachable!(),
        };
        self.set_producer(out, self.nodes.len());
        Ok(self.push_node(Node {
            name: sym.into(),
            inputs: vec![a, b],
            outputs: vec![out],
            alarm_base: 2,
            kernel: Kernel::BinOp { op },
            rank: 0,
        }))
    }

    fn add_filter(&mut self, flag: usize, x: usize, out: usize) -> usize {
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "filter".into(),
            inputs: vec![flag, x],
            outputs: vec![out],
            alarm_base: 2,
            kernel: Kernel::Filter,
            rank: 0,
        })
    }

    fn add_sample(&mut self, trigger: usize, x: usize, out: usize) -> usize {
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "sample".into(),
            inputs: vec![trigger, x],
            outputs: vec![out],
            alarm_base: 2,
            kernel: Kernel::Sample,
            rank: 0,
        })
    }

    fn add_merge(&mut self, a: usize, b: usize, out: usize) -> usize {
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "merge".into(),
            inputs: vec![a, b],
            outputs: vec![out],
            alarm_base: 2,
            kernel: Kernel::Merge,
            rank: 0,
        })
    }

    fn add_count(&mut self, x: usize, out: usize) -> usize {
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "count".into(),
            inputs: vec![x],
            outputs: vec![out],
            alarm_base: 1,
            kernel: Kernel::Count,
            rank: 0,
        })
    }

    fn add_firstn(&mut self, x: usize, out: usize, n: i64) -> usize {
        self.set_producer(out, self.nodes.len());
        self.push_node(Node {
            name: "firstN".into(),
            inputs: vec![x],
            outputs: vec![out],
            alarm_base: 1,
            kernel: Kernel::FirstN { n },
            rank: 0,
        })
    }

    fn add_print(&mut self, name: String, x: usize) -> usize {
        self.push_node(Node {
            name: "print".into(),
            inputs: vec![x],
            outputs: vec![],
            alarm_base: 1,
            kernel: Kernel::Print { name },
            rank: 0,
        })
    }

    fn add_graph_output(&mut self, name: String, x: usize) -> usize {
        self.outputs.entry(name.clone()).or_default();
        self.push_node(Node {
            name: "graph_output".into(),
            inputs: vec![x],
            outputs: vec![],
            alarm_base: 1,
            kernel: Kernel::GraphOutput { name },
            rank: 0,
        })
    }

    /// Register a Python node. `ts_inputs` are the time-series edges; `alarms`
    /// are pre-allocated alarm edges the node may schedule onto.
    #[pyo3(signature = (func, ts_inputs, alarms, outputs, name, run_at_start))]
    fn add_python_node(
        &mut self,
        func: Py<PyAny>,
        ts_inputs: Vec<usize>,
        alarms: Vec<usize>,
        outputs: Vec<usize>,
        name: String,
        run_at_start: bool,
    ) -> usize {
        let alarm_base = ts_inputs.len();
        let mut inputs = ts_inputs;
        inputs.extend(alarms);
        let node_id = self.nodes.len();
        for &o in &outputs {
            self.edges[o].producer = Some(node_id);
        }
        self.push_node(Node {
            name,
            inputs,
            outputs,
            alarm_base,
            kernel: Kernel::Python { func, run_at_start },
            rank: 0,
        })
    }

    /// Describe the built graph for visualization. Returns
    /// `(nodes, producers)` where `nodes` is a list of
    /// `(id, name, rank, input_edges, output_edges)` and `producers` maps each
    /// edge id to the node that produces it (edges with no producer — sources,
    /// timers, alarms, feedback — are omitted).
    fn topology(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        self.finalize()?;
        let nodes = PyList::empty_bound(py);
        for (nid, node) in self.nodes.iter().enumerate() {
            let inputs = PyList::new_bound(py, node.inputs.iter().copied());
            let outputs = PyList::new_bound(py, node.outputs.iter().copied());
            let tup = PyTuple::new_bound(
                py,
                &[
                    nid.into_py(py),
                    node.name.clone().into_py(py),
                    node.rank.into_py(py),
                    inputs.into_py(py),
                    outputs.into_py(py),
                ],
            );
            nodes.append(tup)?;
        }
        let producers = pyo3::types::PyDict::new_bound(py);
        for (eid, e) in self.edges.iter().enumerate() {
            if let Some(p) = e.producer {
                producers.set_item(eid, p)?;
            }
        }
        Ok(PyTuple::new_bound(py, &[nodes.into_py(py), producers.into_py(py)]).into())
    }

    /// Run the engine over `[start_ns, end_ns]`. Returns
    /// `{name: [(time_ns, value), ...]}` for every registered graph output.
    #[pyo3(signature = (start_ns, end_ns, realtime, profile = false))]
    fn run(
        &mut self,
        py: Python<'_>,
        start_ns: i64,
        end_ns: i64,
        realtime: bool,
        profile: bool,
    ) -> PyResult<PyObject> {
        self.endtime = end_ns;
        self.cycle = 0;
        self.profile = profile;
        self.prof_count = vec![0; self.nodes.len()];
        self.prof_ns = vec![0; self.nodes.len()];
        self.finalize()?;
        for v in self.outputs.values_mut() {
            v.clear();
        }

        // Build the consumers map.
        for c in self.consumers.iter_mut() {
            c.clear();
        }
        for (nid, node) in self.nodes.iter().enumerate() {
            for &e in &node.inputs {
                self.consumers[e].push(nid);
            }
        }

        let mut timed: BinaryHeap<Reverse<(i64, u64)>> = BinaryHeap::new();
        let mut payload: HashMap<u64, Sched> = HashMap::new();
        let mut seq: u64 = 0;

        // Seed initial work from source kernels.
        for (nid, node) in self.nodes.iter().enumerate() {
            match &node.kernel {
                Kernel::Const { .. } => {
                    Self::push_sched(&mut timed, &mut payload, &mut seq, start_ns,
                        Sched::Edge(node.inputs[0], Value::Bool(true)));
                }
                Kernel::Timer { interval, .. } => {
                    let t = start_ns + interval;
                    if t <= end_ns {
                        Self::push_sched(&mut timed, &mut payload, &mut seq, t,
                            Sched::Edge(node.inputs[0], Value::Bool(true)));
                    }
                }
                Kernel::Python { run_at_start: true, .. } => {
                    Self::push_sched(&mut timed, &mut payload, &mut seq, start_ns,
                        Sched::Node(nid));
                }
                _ => {}
            }
        }

        if realtime {
            self.run_realtime(py, start_ns, end_ns, &mut timed, &mut payload, &mut seq)?;
        } else {
            while let Some(Reverse((t, _))) = timed.peek().copied() {
                if t > end_ns {
                    break;
                }
                self.process_cycle(py, t, end_ns, &mut timed, &mut payload, &mut seq)?;
            }
        }

        self.collect_outputs(py)
    }
}

impl Engine {
    fn push_node(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.counters.push(0);
        self.nodes.len() - 1
    }

    /// Rebuild the edge→consumers map for the full current node set.
    fn rebuild_consumers(&mut self) {
        for c in self.consumers.iter_mut() {
            c.clear();
        }
        for (nid, node) in self.nodes.iter().enumerate() {
            for &e in &node.inputs {
                self.consumers[e].push(nid);
            }
        }
    }

    /// Seed a source node's initial work relative to `base` into the stepped
    /// scheduler (const at `base`, timer at `base+interval`, python-start now).
    fn seed_source(&mut self, nid: usize, base: i64) {
        let (kind, edge, iv): (u8, usize, i64) = match &self.nodes[nid].kernel {
            Kernel::Const { .. } => (0, self.nodes[nid].inputs[0], 0),
            Kernel::Timer { interval, .. } => (1, self.nodes[nid].inputs[0], *interval),
            Kernel::Python { run_at_start: true, .. } => (2, 0, 0),
            _ => (3, 0, 0),
        };
        match kind {
            0 => Self::push_sched(&mut self.s_timed, &mut self.s_payload, &mut self.s_seq,
                base, Sched::Edge(edge, Value::Bool(true))),
            1 => {
                let t = base + iv;
                if t <= self.s_end {
                    Self::push_sched(&mut self.s_timed, &mut self.s_payload, &mut self.s_seq,
                        t, Sched::Edge(edge, Value::Bool(true)));
                }
            }
            2 => Self::push_sched(&mut self.s_timed, &mut self.s_payload, &mut self.s_seq,
                self.s_now, Sched::Node(nid)),
            _ => {}
        }
    }

    /// Absorb nodes added since the last step: re-rank, re-wire consumers,
    /// grow scratch vectors, and seed any new source nodes at the frontier.
    fn integrate(&mut self) -> PyResult<()> {
        let n = self.nodes.len();
        if n <= self.s_integrated {
            return Ok(());
        }
        self.finalize()?;
        self.prof_count.resize(n, 0);
        self.prof_ns.resize(n, 0);
        self.rebuild_consumers();
        for nid in self.s_integrated..n {
            self.seed_source(nid, self.s_now);
        }
        self.s_integrated = n;
        Ok(())
    }

    /// Marshal collected graph outputs into `{name: [(time_ns, value), ...]}`.
    fn collect_outputs(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = pyo3::types::PyDict::new_bound(py);
        for (name, rows) in self.outputs.iter() {
            let list = PyList::empty_bound(py);
            for (t, v) in rows {
                let tup = PyTuple::new_bound(py, &[t.into_py(py), v.clone_ref(py)]);
                list.append(tup)?;
            }
            dict.set_item(name, list)?;
        }
        Ok(dict.into())
    }

    fn set_producer(&mut self, edge: usize, node_id: usize) {
        self.edges[edge].producer = Some(node_id);
    }

    fn push_sched(
        timed: &mut BinaryHeap<Reverse<(i64, u64)>>,
        payload: &mut HashMap<u64, Sched>,
        seq: &mut u64,
        time: i64,
        sched: Sched,
    ) {
        *seq += 1;
        payload.insert(*seq, sched);
        timed.push(Reverse((time, *seq)));
    }

    /// Compute topological ranks; error on direct cycles (express loops with
    /// `rcsp.feedback`, which breaks the cycle with a one-cycle delay).
    fn finalize(&mut self) -> PyResult<()> {
        let n = self.nodes.len();
        let mut rank = vec![-1i64; n];
        let mut visiting = vec![false; n];
        // Snapshot producers per node input to avoid borrow gymnastics.
        let inputs: Vec<Vec<usize>> = self.nodes.iter().map(|nd| nd.inputs.clone()).collect();
        let producers: Vec<Option<usize>> = self.edges.iter().map(|e| e.producer).collect();

        fn compute(
            i: usize,
            rank: &mut Vec<i64>,
            visiting: &mut Vec<bool>,
            inputs: &Vec<Vec<usize>>,
            producers: &Vec<Option<usize>>,
        ) -> PyResult<i64> {
            if rank[i] >= 0 {
                return Ok(rank[i]);
            }
            if visiting[i] {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "cycle detected in graph; express loops with rcsp.feedback",
                ));
            }
            visiting[i] = true;
            let mut r = 0i64;
            for &e in &inputs[i] {
                if let Some(p) = producers[e] {
                    r = r.max(compute(p, rank, visiting, inputs, producers)? + 1);
                }
            }
            visiting[i] = false;
            rank[i] = r;
            Ok(r)
        }

        for i in 0..n {
            compute(i, &mut rank, &mut visiting, &inputs, &producers)?;
        }
        for (i, nd) in self.nodes.iter_mut().enumerate() {
            nd.rank = rank[i];
        }
        Ok(())
    }

    /// Process one engine cycle at time `now`: drain injections scheduled at
    /// `now`, then fire dirty nodes in rank order (glitch-free), applying their
    /// emissions within the cycle and their alarms to the future queue.
    fn process_cycle(
        &mut self,
        py: Python<'_>,
        now: i64,
        end_ns: i64,
        timed: &mut BinaryHeap<Reverse<(i64, u64)>>,
        payload: &mut HashMap<u64, Sched>,
        seq: &mut u64,
    ) -> PyResult<()> {
        // Each cycle gets a unique id so `ticked` is per-cycle, not per-
        // timestamp — essential when feedback drives several cycles at one time.
        self.cycle += 1;
        let cyc = self.cycle;

        let mut cycle: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();
        let mut queued: HashSet<usize> = HashSet::new();
        let mut ran: HashSet<usize> = HashSet::new();

        while let Some(Reverse((tt, _))) = timed.peek().copied() {
            if tt != now {
                break;
            }
            let Reverse((_, s)) = timed.pop().unwrap();
            match payload.remove(&s).unwrap() {
                Sched::Edge(edge, val) => {
                    self.edges[edge].value = Some(val);
                    self.edges[edge].last_tick = Some(cyc);
                    for &c in &self.consumers[edge] {
                        if queued.insert(c) {
                            cycle.push(Reverse((self.nodes[c].rank, c)));
                        }
                    }
                }
                Sched::Node(nid) => {
                    if queued.insert(nid) {
                        cycle.push(Reverse((self.nodes[nid].rank, nid)));
                    }
                }
            }
        }

        while let Some(Reverse((_, nid))) = cycle.pop() {
            if !ran.insert(nid) {
                continue;
            }
            let t0 = if self.profile {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let (emits, futures) = self.run_node(py, nid, now, cyc)?;
            if let Some(t0) = t0 {
                self.prof_count[nid] += 1;
                self.prof_ns[nid] += t0.elapsed().as_nanos();
            }
            for (edge, val) in emits {
                self.edges[edge].value = Some(val);
                self.edges[edge].last_tick = Some(cyc);
                for &c in &self.consumers[edge] {
                    if !ran.contains(&c) && queued.insert(c) {
                        cycle.push(Reverse((self.nodes[c].rank, c)));
                    }
                }
            }
            for (edge, ftime, val) in futures {
                if ftime <= end_ns {
                    Self::push_sched(timed, payload, seq, ftime, Sched::Edge(edge, val));
                }
            }
        }
        Ok(())
    }

    /// Realtime driver: pace to the wall clock, draining registered push
    /// adapters (external threads placing items on a `queue.Queue`) and firing
    /// timed events as they come due, until the run's duration elapses.
    fn run_realtime(
        &mut self,
        py: Python<'_>,
        start_ns: i64,
        end_ns: i64,
        timed: &mut BinaryHeap<Reverse<(i64, u64)>>,
        payload: &mut HashMap<u64, Sched>,
        seq: &mut u64,
    ) -> PyResult<()> {
        let wall_start = std::time::Instant::now();
        let duration_ns = (end_ns - start_ns).max(0) as u128;
        // Strictly-increasing timestamp for pushed items so a burst arriving at
        // one wall-clock instant lands in distinct cycles instead of colliding
        // on the edge (which holds one value per cycle).
        let mut last_push: i64 = start_ns - 1;

        loop {
            let elapsed = wall_start.elapsed().as_nanos();
            let now_ns = start_ns + elapsed.min(duration_ns) as i64;

            // Drain external push adapters, injecting at wall-clock arrival time.
            for i in 0..self.push_adapters.len() {
                let edge = self.push_adapters[i].0;
                loop {
                    let item = {
                        let q = self.push_adapters[i].1.bind(py);
                        match q.call_method0("get_nowait") {
                            Ok(item) => item,
                            Err(_) => break, // queue.Empty
                        }
                    };
                    let val = Value::from_py(&item);
                    let t = now_ns.max(last_push + 1);
                    last_push = t;
                    Self::push_sched(timed, payload, seq, t, Sched::Edge(edge, val));
                }
            }

            // Fire everything now due (including this iteration's burst, whose
            // synthetic timestamps may sit just past now_ns).
            let fire_until = now_ns.max(last_push);
            while let Some(Reverse((t, _))) = timed.peek().copied() {
                if t > fire_until || t > end_ns {
                    break;
                }
                self.process_cycle(py, t, end_ns, timed, payload, seq)?;
            }

            if elapsed >= duration_ns {
                break;
            }
            // Sleep briefly, releasing the GIL so pusher threads can enqueue.
            py.allow_threads(|| std::thread::sleep(std::time::Duration::from_millis(1)));
        }
        Ok(())
    }

    /// Run a single node at time `now` in cycle `cyc`, returning
    /// `(immediate emits, future injections)`. `cyc` drives `ticked`; `now` is
    /// the timestamp used for emissions, outputs and alarm scheduling.
    fn run_node(
        &mut self,
        py: Python<'_>,
        nid: usize,
        now: i64,
        cyc: i64,
    ) -> PyResult<(Vec<(usize, Value)>, Vec<(usize, i64, Value)>)> {
        // Snapshot the pieces we need so we don't hold a borrow of self.nodes.
        let inputs = self.nodes[nid].inputs.clone();
        let outputs = self.nodes[nid].outputs.clone();
        let alarm_base = self.nodes[nid].alarm_base;

        let ticked = |e: usize, edges: &Vec<EdgeState>| edges[e].last_tick == Some(cyc);
        let mut emits: Vec<(usize, Value)> = Vec::new();
        let mut futures: Vec<(usize, i64, Value)> = Vec::new();

        match &self.nodes[nid].kernel {
            Kernel::Const { value } => {
                emits.push((outputs[0], value.clone()));
            }
            Kernel::Timer { interval, value } => {
                emits.push((outputs[0], value.clone()));
                futures.push((inputs[0], now + interval, Value::Bool(true)));
            }
            Kernel::Delay { delta } => {
                let x = inputs[0];
                let alarm = inputs[1];
                if ticked(x, &self.edges) {
                    if let Some(v) = &self.edges[x].value {
                        futures.push((alarm, now + delta, v.clone()));
                    }
                }
                if ticked(alarm, &self.edges) {
                    if let Some(v) = &self.edges[alarm].value {
                        emits.push((outputs[0], v.clone()));
                    }
                }
            }
            Kernel::Count => {
                self.counters[nid] += 1;
                emits.push((outputs[0], Value::Int(self.counters[nid])));
            }
            Kernel::FirstN { n } => {
                if self.counters[nid] < *n {
                    self.counters[nid] += 1;
                    if let Some(v) = &self.edges[inputs[0]].value {
                        emits.push((outputs[0], v.clone()));
                    }
                }
            }
            Kernel::BinOp { op } => {
                let a = self.edges[inputs[0]].value.clone();
                let b = self.edges[inputs[1]].value.clone();
                if let (Some(a), Some(b)) = (a, b) {
                    if let Some(v) = apply_binop(*op, &a, &b) {
                        emits.push((outputs[0], v));
                    }
                }
            }
            Kernel::Filter => {
                let flag = inputs[0];
                let x = inputs[1];
                let pass = self.edges[flag].value.as_ref().map(|v| v.is_truthy()).unwrap_or(false);
                if ticked(x, &self.edges) && pass {
                    if let Some(v) = &self.edges[x].value {
                        emits.push((outputs[0], v.clone()));
                    }
                }
            }
            Kernel::Sample => {
                let trigger = inputs[0];
                let x = inputs[1];
                if ticked(trigger, &self.edges) {
                    if let Some(v) = &self.edges[x].value {
                        emits.push((outputs[0], v.clone()));
                    }
                }
            }
            Kernel::Merge => {
                // Prefer the first input when both tick in the same cycle.
                let a = inputs[0];
                let b = inputs[1];
                if ticked(a, &self.edges) {
                    if let Some(v) = &self.edges[a].value {
                        emits.push((outputs[0], v.clone()));
                    }
                } else if ticked(b, &self.edges) {
                    if let Some(v) = &self.edges[b].value {
                        emits.push((outputs[0], v.clone()));
                    }
                }
            }
            Kernel::Print { name } => {
                if let Some(v) = &self.edges[inputs[0]].value {
                    let obj = v.to_py(py);
                    let s: String = obj.bind(py).str()?.extract()?;
                    println!("{} {} {}", format_time(now), name, s);
                }
            }
            Kernel::GraphOutput { name } => {
                if let Some(v) = &self.edges[inputs[0]].value {
                    let obj = v.to_py(py);
                    self.outputs.get_mut(name).unwrap().push((now, obj));
                }
            }
            Kernel::Python { func, .. } => {
                let func = func.clone_ref(py);
                // Build (values, ticked, valid) arrays for every input.
                let vals = PyList::empty_bound(py);
                let ticks = PyList::empty_bound(py);
                let valids = PyList::empty_bound(py);
                for &e in &inputs {
                    let st = &self.edges[e];
                    match &st.value {
                        Some(v) => vals.append(v.to_py(py))?,
                        None => vals.append(py.None())?,
                    }
                    ticks.append(st.last_tick == Some(cyc))?;
                    valids.append(st.value.is_some())?;
                }
                let ret = func.call1(py, (now, &vals, &ticks, &valids))?;
                let ret = ret.bind(py);
                // ret == (emissions, alarms)
                let emissions = ret.get_item(0)?;
                for item in emissions.iter()? {
                    let item = item?;
                    let idx: usize = item.get_item(0)?.extract()?;
                    let val = Value::from_py(&item.get_item(1)?);
                    emits.push((outputs[idx], val));
                }
                let alarms = ret.get_item(1)?;
                for item in alarms.iter()? {
                    let item = item?;
                    let aidx: usize = item.get_item(0)?.extract()?;
                    let delay: i64 = item.get_item(1)?.extract()?;
                    let val = Value::from_py(&item.get_item(2)?);
                    let edge = inputs[alarm_base + aidx];
                    futures.push((edge, now + delay, val));
                }
            }
        }
        Ok((emits, futures))
    }
}

fn apply_binop(op: BinOp, a: &Value, b: &Value) -> Option<Value> {
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
            // Preserve integer arithmetic when both sides are integers.
            if let (Value::Int(x), Value::Int(y)) = (a, b) {
                return Some(match op {
                    BinOp::Add => Value::Int(x + y),
                    BinOp::Sub => Value::Int(x - y),
                    BinOp::Mul => Value::Int(x * y),
                    BinOp::Div => Value::Float(*x as f64 / *y as f64),
                    _ => unreachable!(),
                });
            }
            let (x, y) = (a.as_f64()?, b.as_f64()?);
            Some(match op {
                BinOp::Add => Value::Float(x + y),
                BinOp::Sub => Value::Float(x - y),
                BinOp::Mul => Value::Float(x * y),
                BinOp::Div => Value::Float(x / y),
                _ => unreachable!(),
            })
        }
        BinOp::Gt | BinOp::Lt | BinOp::Ge | BinOp::Le | BinOp::Eq | BinOp::Ne => {
            let (x, y) = (a.as_f64()?, b.as_f64()?);
            Some(Value::Bool(match op {
                BinOp::Gt => x > y,
                BinOp::Lt => x < y,
                BinOp::Ge => x >= y,
                BinOp::Le => x <= y,
                BinOp::Eq => x == y,
                BinOp::Ne => x != y,
                _ => unreachable!(),
            }))
        }
    }
}

fn format_time(ns: i64) -> String {
    let secs = ns / 1_000_000_000;
    let sub = ns % 1_000_000_000;
    format!("{}.{:09}", secs, sub)
}

#[pymodule]
fn _rcsp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    Ok(())
}
