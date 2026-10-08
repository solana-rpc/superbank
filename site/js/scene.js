// Isometric Three.js view of a buildTopology() result.
// update() reconciles in place: nodes, edges and zones are diffed by id, removed
// objects fade out and are disposed, and particles come from fixed-size pools
// so a long-running page never allocates per frame. Geometries are shared
// through a keyed cache whose key space is bounded by the topology, so toggling
// selectors cannot grow GPU memory without bound.

import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { CSS2DObject, CSS2DRenderer } from 'three/addons/renderers/CSS2DRenderer.js';
import { RoundedBoxGeometry } from 'three/addons/geometries/RoundedBoxGeometry.js';

// Solana brand purple/green plus the blue from the official gradient
// (solana.com/branding); orange (Parquet) and yellow (ClickHouse) are data
// colours kept from the component brands.
const PALETTE = Object.freeze({
  purple: '#9945FF',
  blue: '#64A8F2',
  green: '#14F195',
  orange: '#F59E0B',
  yellow: '#FACC15',
  amber: '#F59E0B',
  slate: '#848895',
  pulse: '#FFFFFF',
});

// query: a SQL statement one process sends another (solparq -> ClickHouse);
// meta: small bundle files (manifest, report, checksums, done marker).
const PARTICLE_COLOR = {
  block: PALETTE.purple,
  rows: PALETTE.blue,
  index: PALETTE.green,
  parquet: PALETTE.orange,
  query: '#EC4899',
  meta: '#ABABBA',
};
const PARTICLE_SIZE = {
  block: [0.24, 0.24, 0.24],
  rows: [0.18, 0.18, 0.18],
  index: [0.12, 0.12, 0.12],
  parquet: [0.3, 0.07, 0.22],
  query: [0.26, 0.26, 0.26],
  meta: [0.16, 0.05, 0.12],
};
const TIER_COLOR = { 'head-cache': PALETTE.purple, 'disk-cache': PALETTE.green, ch: PALETTE.yellow };
const STATUS_COLOR = { ok: PALETTE.green, warn: PALETTE.amber, info: PALETTE.blue };
const PROCESS_COLOR = {
  superbank: PALETTE.purple,
  jetstreamer: '#B57BFF',
  rpc: PALETTE.blue,
  solparq: PALETTE.orange,
  verify: PALETTE.slate,
};
const ENDPOINT_COLOR = {
  grpc: PALETTE.purple,
  fumarole: '#EF4444',
  jsonrpc: PALETTE.blue,
  bigtable: '#80ECFF',
  oldfaithful: '#D97706',
};
const ZONE_TINT = {
  upstream: PALETTE.purple,
  ingest: PALETTE.purple,
  clickhouse: PALETTE.yellow,
  serve: PALETTE.blue,
  archive: PALETTE.orange,
  verify: PALETTE.green,
};

// The page is dark only (like solana.com), so there is a single theme tuned
// for a black background.
const THEME = Object.freeze({
  hemiSky: '#ece8ff',
  hemiGround: '#1a1726',
  hemi: 1.6,
  sun: 1.3,
  // Lifted off pure black so the slabs read against the page.
  platform: '#16161D',
  // Surfaces stay near-monochrome like solana.com/data; colour is for data.
  zoneTint: 0.05,
  edgeNeutral: '#5C5C70',
  edgeMix: 0.65,
  shadow: 0.5,
  metal: '#9AA0B0',
  dark: '#1D1D26',
  shell: '#2B2B38',
  // Lighter than the platform so the server blocks read on a dark page.
  server: '#4D4D66',
  // Yellow over near-black turns muddy; keep the ClickHouse tint faint.
  tintScale: { clickhouse: 0.45 },
});

// Union of every zone rect topology.js can emit, plus head room for the
// tallest node and its label. The pose (landscape/portrait) is chosen against
// this so it never flips on a toggle; the frame fits only the zones on screen.
const BOUNDS = { u0: -17, u1: 17.6, v0: -7.2, v1: 7.8, y0: -0.3, y1: 2.6 };

const ZONE_H = 0.28;
const FADE_S = 0.3;
const CAMERA_TWEEN_S = 0.45;
const CLICK_SLOP_PX = 5;
const TOOLTIP_OFFSET_PX = 12;
const EDGE_HOVER_PX = 9;
const EDGE_SAMPLES = 24;
const MAX_CUBES = 400;
const MAX_SPHERES = 150;
const MAX_HOPPER = 12;
const MAX_LEGS = 10;
const MAX_RELAY_DEPTH = 4;
const BURST_GAP_S = 0.12;
const FLUSH_STAGGER_S = 0.35;
const JOURNEY_RATE = 1.2;
const JOURNEY_SPEED = 6;
const MAX_DT = 0.1;
const CAMERA_DIST = 80;
const NODE_YAW = Math.PI / 4;
// Logical (u, v) is laid out on a ground plane rotated 45 degrees, so with the
// camera on the (+1, +1, +1) diagonal `u` runs screen-left -> right and `v`
// runs back -> front.
const LAYOUT_YAW = Math.PI / 4;
const POSES = {
  landscape: new THREE.Vector3(1, 1, 1).normalize(),
  // Narrow containers turn the camera a quarter so the long `u` axis runs
  // top -> bottom; still a true isometric angle.
  portrait: new THREE.Vector3(1, 1, -1).normalize(),
};
const UP = new THREE.Vector3(0, 1, 0);
const EMPTY = Object.freeze([]);

const LABEL_GAP_PX = 4;
// Above this placement cost a label tries dropping its sublabel instead.
const LABEL_COMPACT_COST = 0.6;
// Preferred label sides, best first. Endpoints and tables stand in tight
// columns, so their labels go beside them rather than over the neighbour.
const LABEL_SIDES = {
  default: ['top', 'right', 'left', 'bottom', 'topRight', 'topLeft'],
  endpoint: ['right', 'left', 'top', 'topRight', 'bottom', 'topLeft'],
  table: ['right', 'left', 'top', 'bottom', 'topRight', 'topLeft'],
};
// CSS2DObject.center for each side: which point of the label sits on the anchor.
const LABEL_CENTER = {
  top: [0.5, 1],
  bottom: [0.5, 0],
  right: [0, 0.5],
  left: [1, 0.5],
  topRight: [0, 1],
  topLeft: [1, 1],
};
// Hiding part of a machine costs more than hiding part of a table or tower.
const COVER_WEIGHT = { process: 2, cluster: 2, network: 2, store: 2, memory: 2, localdb: 2 };
// Lower is placed first and so wins contested space. The least flexible
// labels (crowded columns) go first; big isolated nodes adapt around them.
const LABEL_PRIORITY = { endpoint: 0, table: 1, coordinator: 2, memory: 2, localdb: 2, clients: 2, store: 2, network: 2, process: 3, cluster: 3 };

function webgl2Available() {
  try {
    const probe = document.createElement('canvas');
    const gl = probe.getContext('webgl2');
    if (!gl) return false;
    gl.getExtension('WEBGL_lose_context')?.loseContext();
    return true;
  } catch {
    return false;
  }
}

const easeOut = (x) => 1 - (1 - x) ** 3;
const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));
const jitter = (base) => base * (0.55 + Math.random() * 0.9);

export function createScene(container, { onSelect, reducedMotion = false, debug = false } = {}) {
  if (!container) throw new Error('createScene: container is required');
  if (!webgl2Available()) throw new Error('WebGL unavailable');

  let renderer;
  try {
    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  } catch {
    throw new Error('WebGL unavailable');
  }
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.setClearColor(0x000000, 0);

  const canvas = renderer.domElement;
  canvas.className = 'scene-canvas';
  Object.assign(canvas.style, { position: 'absolute', inset: '0', width: '100%', height: '100%', display: 'block' });

  const labelRenderer = new CSS2DRenderer();
  const overlay = labelRenderer.domElement;
  overlay.className = 'scene-labels';
  // CSS2DRenderer gives labels z-indexes; isolate them so the edge tooltip and
  // any page controls later in the container paint above every label.
  Object.assign(overlay.style, { position: 'absolute', inset: '0', pointerEvents: 'none', isolation: 'isolate' });

  const tooltip = document.createElement('div');
  tooltip.className = 'edge-tooltip';
  tooltip.setAttribute('role', 'tooltip');
  Object.assign(tooltip.style, { position: 'absolute', left: '0', top: '0', transform: 'none', pointerEvents: 'none', display: 'none' });
  // Prepend so controls the page already placed in the container stay on top.
  container.prepend(canvas, overlay, tooltip);

  // Zero-specificity defaults for zone labels, which are not part of the page
  // contract; styles.css can override them without !important.
  const defaults = document.createElement('style');
  defaults.textContent = [
    ':where(.zone-label){font:600 10px/1 ui-monospace,monospace;letter-spacing:.08em;text-transform:uppercase;',
    'color:#848895;white-space:nowrap;pointer-events:none;user-select:none}',
  ].join('');
  document.head.appendChild(defaults);

  const scene = new THREE.Scene();
  const camera = new THREE.OrthographicCamera(-10, 10, 10, -10, 0.1, CAMERA_DIST * 4);
  // Connected to the canvas further down, after our own pointer listeners, so
  // onPointerDown sees the camera state from before OrbitControls' 'start'.
  const controls = new OrbitControls(camera);
  controls.enableDamping = !reducedMotion;
  controls.dampingFactor = 0.09;
  controls.minZoom = 0.6;
  controls.maxZoom = 5;
  controls.minPolarAngle = 0.3;
  controls.maxPolarAngle = 1.2;
  controls.screenSpacePanning = true;
  controls.zoomToCursor = true;
  // On touch-first devices one finger scrolls the page (the stage fills most of
  // a phone screen) and two fingers pan and zoom the scene. A touches.ONE value
  // that is not a TOUCH constant makes OrbitControls ignore one-finger drags.
  const coarsePointer = window.matchMedia?.('(pointer: coarse)').matches ?? false;
  if (coarsePointer) controls.touches.ONE = null;

  const hemi = new THREE.HemisphereLight();
  const sun = new THREE.DirectionalLight();
  sun.position.set(-8, 16, 10);
  scene.add(hemi, sun);

  const layout = new THREE.Group();
  layout.rotation.y = LAYOUT_YAW;
  scene.add(layout);
  layout.updateMatrixWorld(true);

  const theme = THEME;

  // --- Shared resources ----------------------------------------------------
  const geoCache = new Map();
  const geo = (key, make) => {
    let g = geoCache.get(key);
    if (!g) {
      g = make();
      geoCache.set(key, g);
    }
    return g;
  };
  const box = (w, h, d) => geo(`box:${w}:${h}:${d}`, () => new THREE.BoxGeometry(w, h, d));
  const rbox = (w, h, d, r) => geo(`rbox:${w}:${h}:${d}:${r}`, () => new RoundedBoxGeometry(w, h, d, 2, r));
  const cyl = (rt, rb, h, seg, open = false) =>
    geo(`cyl:${rt}:${rb}:${h}:${seg}:${open}`, () => new THREE.CylinderGeometry(rt, rb, h, seg, 1, open));
  const sphere = (r, ws = 16, hs = 12) => geo(`sph:${r}:${ws}:${hs}`, () => new THREE.SphereGeometry(r, ws, hs));
  const torus = (R, r, rs = 8, ts = 32) => geo(`tor:${R}:${r}:${rs}:${ts}`, () => new THREE.TorusGeometry(R, r, rs, ts));
  const cone = (r, h, seg = 16) => geo(`cone:${r}:${h}:${seg}`, () => new THREE.ConeGeometry(r, h, seg));
  const edgesOf = (key, source) => geo(`edges:${key}`, () => new THREE.EdgesGeometry(source, 20));
  const flatPlane = () =>
    geo('plane:flat', () => {
      const g = new THREE.PlaneGeometry(1, 1);
      g.rotateX(-Math.PI / 2);
      return g;
    });
  const ring = () =>
    geo('ring:select', () => {
      const g = new THREE.RingGeometry(0.86, 1, 48);
      g.rotateX(-Math.PI / 2);
      return g;
    });

  const shadowTex = makeShadowTexture();
  const dashTex = makeDashTexture();

  // --- Particle pools ------------------------------------------------------
  const cubeMat = new THREE.MeshStandardMaterial({ roughness: 0.45, metalness: 0 });
  const cubeMesh = new THREE.InstancedMesh(box(1, 1, 1), cubeMat, MAX_CUBES);
  const sphereMat = new THREE.MeshStandardMaterial({ roughness: 0.3, metalness: 0 });
  const sphereMesh = new THREE.InstancedMesh(sphere(1, 16, 12), sphereMat, MAX_SPHERES);
  const hopperMat = new THREE.MeshStandardMaterial({ color: PALETTE.purple, roughness: 0.45 });
  const hopperMesh = new THREE.InstancedMesh(box(1, 1, 1), hopperMat, MAX_HOPPER);
  for (const mesh of [cubeMesh, sphereMesh, hopperMesh]) {
    mesh.frustumCulled = false;
    mesh.count = 0;
    mesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    layout.add(mesh);
  }
  // Allocate the colour attributes up front so setColorAt never allocates.
  cubeMesh.setColorAt(0, new THREE.Color(1, 1, 1));
  sphereMesh.setColorAt(0, new THREE.Color(1, 1, 1));
  cubeMesh.instanceColor.setUsage(THREE.DynamicDrawUsage);
  sphereMesh.instanceColor.setUsage(THREE.DynamicDrawUsage);

  const particleColors = Object.fromEntries(Object.entries(PARTICLE_COLOR).map(([k, c]) => [k, new THREE.Color(c)]));
  const pulseColor = new THREE.Color(PALETTE.pulse);
  const cubes = Array.from({ length: MAX_CUBES }, () => ({ edge: null, t: 0, type: 'block', depth: 0, spin: 0, spinRate: 0 }));
  let cubeCount = 0;
  const spheres = Array.from({ length: MAX_SPHERES }, () => ({
    legEdge: new Array(MAX_LEGS).fill(null),
    legDir: new Int8Array(MAX_LEGS),
    n: 0,
    i: 0,
    t: 0,
    serveLeg: -1,
    color: new THREE.Color(),
    tierColor: new THREE.Color(),
  }));
  let sphereCount = 0;

  const selectRingMat = new THREE.MeshBasicMaterial({ color: PALETTE.purple, transparent: true, opacity: 0.85, depthWrite: false });
  const selectRing = new THREE.Mesh(ring(), selectRingMat);
  selectRing.visible = false;
  selectRing.renderOrder = 2;

  // --- Scratch objects (reused every frame) -------------------------------
  const tmpPos = new THREE.Vector3();
  const tmpVec = new THREE.Vector3();
  const tmpScale = new THREE.Vector3();
  const tmpQuat = new THREE.Quaternion();
  const tmpEuler = new THREE.Euler();
  const tmpMat = new THREE.Matrix4();
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  // --- State ---------------------------------------------------------------
  const nodeViews = new Map();
  const edgeViews = new Map();
  const zoneViews = new Map();
  let outEdges = new Map();
  let pickables = [];
  let topology = null;
  let selectedId = null;
  let hoverNodeId = null;
  let hoverEdgeId = null;
  let simTime = 0;
  let journeyWait = 0;
  let paused = false;
  let hidden = document.hidden;
  let inView = true;
  let disposed = false;
  let loopOn = false;
  let lastNow = 0;
  let size = { width: 0, height: 0 };
  let pose = 'landscape';
  let userMoved = false;
  // userMoved as it was when the current press began; a press that turns out
  // to be a click puts it back.
  let movedBeforeGesture = false;
  let focusId = null;
  let baseHalfH = 10;

  // --- Materials -----------------------------------------------------------
  // Node materials are per node (fade and hover differ per node);
  // geometries are shared through the cache.
  function makeBuilder(group, mats) {
    const mk = (color, opts = {}) => {
      const { ghost = false, opacity = 0.38, emissive = null, emissiveIntensity = 1, side, roughness = 0.55, metalness = 0.05 } = opts;
      const m = new THREE.MeshStandardMaterial({ color, roughness, metalness, side: side ?? THREE.FrontSide });
      if (emissive) {
        m.emissive.set(emissive);
        m.emissiveIntensity = emissiveIntensity;
      }
      m.userData.base = m.color.clone();
      m.userData.baseEmissive = m.emissive.clone();
      m.userData.baseOpacity = ghost ? opacity : 1;
      mats.push(m);
      return m;
    };
    const line = (color, opacity = 0.9) => {
      const m = new THREE.LineBasicMaterial({ color, transparent: true, opacity });
      m.userData.base = m.color.clone();
      m.userData.baseOpacity = opacity;
      m.userData.isLine = true;
      mats.push(m);
      return m;
    };
    const add = (geometry, material, x = 0, y = 0, z = 0, parent = group) => {
      const mesh = new THREE.Mesh(geometry, material);
      mesh.position.set(x, y, z);
      parent.add(mesh);
      return mesh;
    };
    const pivot = (yaw = NODE_YAW, x = 0, z = 0) => {
      const p = new THREE.Group();
      p.rotation.y = yaw;
      p.position.set(x, 0, z);
      group.add(p);
      return p;
    };
    return { mk, line, add, pivot, group };
  }

  // --- Node shapes ---------------------------------------------------------
  // Each builder adds meshes to b.group (origin = node centre on the ground)
  // and returns the anchors edges and labels attach to.
  function buildNetwork(b) {
    b.add(cyl(1.2, 1.3, 0.16, 40), b.mk(theme.shell, { roughness: 0.8 }), 0, 0.08, 0);
    b.add(cyl(1.02, 1.02, 0.03, 40), b.mk(theme.dark, { roughness: 0.9 }), 0, 0.175, 0);
    const heights = [0.95, 0.6, 0.8, 0.52, 0.74, 0.64, 0.86];
    const from = new THREE.Color('#9945FF');
    const to = new THREE.Color('#14F195');
    const cap = b.mk('#F1F5F9', { emissive: '#F1F5F9', emissiveIntensity: 0.25 });
    heights.forEach((h, i) => {
      const a = ((i - 1) / 6) * Math.PI * 2 + Math.PI / 6;
      const r = i === 0 ? 0 : 0.66;
      const x = Math.cos(a) * r;
      const z = Math.sin(a) * r;
      const p = b.pivot(NODE_YAW, x, z);
      const pillar = b.add(box(0.24, 1, 0.24), b.mk(from.clone().lerp(to, i / 6), { roughness: 0.4 }), 0, 0.19 + h / 2, 0, p);
      pillar.scale.y = h;
      b.add(box(0.27, 0.05, 0.27), cap, 0, 0.19 + h + 0.025, 0, p);
    });
    return { radius: 1.3, height: 1.2, inH: 0.6, outH: 0.95 };
  }

  function buildEndpoint(node, b) {
    const color = ENDPOINT_COLOR[node.variant] ?? PALETTE.slate;
    b.add(cyl(0.4, 0.46, 0.14, 28), b.mk(theme.shell, { roughness: 0.8 }), 0, 0.07, 0);
    b.add(cyl(0.05, 0.09, 0.82, 12), b.mk(theme.metal, { metalness: 0.3, roughness: 0.4 }), 0, 0.14 + 0.41, 0);
    const accent = b.mk(color, { roughness: 0.35, emissive: color, emissiveIntensity: 0.12 });
    const y = 1.0;
    switch (node.variant) {
      case 'grpc': {
        const r1 = b.add(torus(0.24, 0.035, 8, 32), accent, 0, y, 0);
        r1.rotation.x = Math.PI / 2;
        const r2 = b.add(torus(0.16, 0.035, 8, 28), accent, 0, y + 0.14, 0);
        r2.rotation.x = Math.PI / 2;
        b.add(sphere(0.08), accent, 0, y + 0.27, 0);
        break;
      }
      case 'fumarole': {
        b.add(cone(0.3, 0.32, 20), accent, 0, y + 0.06, 0);
        const puff = b.mk('#F8FAFC', { roughness: 0.9 });
        b.add(sphere(0.09), puff, 0.02, y + 0.3, 0);
        b.add(sphere(0.065), puff, -0.06, y + 0.42, 0.03);
        break;
      }
      case 'jsonrpc': {
        const dish = b.add(
          geo('dish', () => new THREE.SphereGeometry(0.3, 24, 8, 0, Math.PI * 2, 0, Math.PI / 2.6)),
          b.mk(color, { roughness: 0.35, side: THREE.DoubleSide }),
          0,
          y + 0.32,
          0,
        );
        dish.rotation.x = Math.PI - 0.5;
        dish.rotation.y = 0.6;
        b.add(sphere(0.05), accent, 0, y + 0.1, 0);
        break;
      }
      case 'bigtable': {
        for (let i = 0; i < 3; i++) {
          const p = b.pivot(NODE_YAW + i * 0.25);
          b.add(rbox(0.42, 0.08, 0.42, 0.02), accent, 0, y + i * 0.11, 0, p);
        }
        break;
      }
      case 'oldfaithful': {
        b.add(cyl(0.12, 0.26, 0.22, 16), accent, 0, y + 0.02, 0);
        const water = b.mk('#7DD3FC', { roughness: 0.2, emissive: '#7DD3FC', emissiveIntensity: 0.2 });
        b.add(cyl(0.05, 0.08, 0.3, 10), water, 0, y + 0.27, 0);
        b.add(sphere(0.08), water, 0, y + 0.45, 0);
        break;
      }
      default:
        b.add(sphere(0.18), accent, 0, y + 0.1, 0);
    }
    return { radius: 0.5, height: 1.48, inH: 0.55, outH: 1.1 };
  }

  function buildProcess(node, b) {
    const color = PROCESS_COLOR[node.variant] ?? PALETTE.slate;
    const verify = node.variant === 'verify';
    const w = verify ? 1.0 : 1.3;
    const h = verify ? 0.62 : 0.8;
    const d = verify ? 0.8 : 1.0;
    const p = b.pivot();
    b.add(rbox(w + 0.14, 0.08, d + 0.14, 0.03), b.mk(theme.shell, { roughness: 0.8 }), 0, 0.04, 0, p);
    b.add(rbox(w, h, d, 0.07), b.mk(color, { roughness: 0.45 }), 0, 0.08 + h / 2, 0, p);
    const panel = b.mk(new THREE.Color(color).lerp(new THREE.Color('#0F172A'), 0.45), { roughness: 0.7 });
    b.add(box(w * 0.68, h * 0.3, 0.02), panel, 0, 0.08 + h * 0.55, d / 2 + 0.005, p);
    const led = b.mk('#E2E8F0', { emissive: '#E2E8F0', emissiveIntensity: 0.7 });
    for (let i = 0; i < 3; i++) b.add(box(0.02, 0.05, 0.1), led, w / 2 + 0.005, 0.08 + h * 0.7, -0.22 + i * 0.16, p);
    const bodyTop = 0.08 + h;
    let top = bodyTop;
    const spec = { radius: verify ? 0.75 : 0.95, height: 0, inH: bodyTop * 0.6, outH: bodyTop, hopper: null, statusMat: null };
    if (node.buffer) {
      const hopperH = 0.44;
      const funnel = b.add(
        cyl(0.56, 0.3, hopperH, 4, true),
        // Glass, so the buffered blocks stacking inside stay visible.
        b.mk(new THREE.Color(color).lerp(new THREE.Color('#ffffff'), 0.55), { ghost: true, opacity: 0.45, side: THREE.DoubleSide, roughness: 0.2 }),
        0,
        bodyTop + hopperH / 2,
        0,
        p,
      );
      funnel.rotation.y = Math.PI / 4;
      top = bodyTop + hopperH;
      spec.hopper = { y: bodyTop + 0.02 };
      spec.inH = top + 0.05;
      spec.outH = bodyTop * 0.7;
    }
    if (verify) {
      b.add(cyl(0.025, 0.025, 0.16, 8), b.mk(theme.metal), 0, bodyTop + 0.08, 0);
      const status = STATUS_COLOR[node.status] ?? PALETTE.slate;
      spec.statusMat = b.mk(status, { emissive: status, emissiveIntensity: 0.55, roughness: 0.3 });
      b.add(sphere(0.13), spec.statusMat, 0, bodyTop + 0.26, 0);
      top = bodyTop + 0.39;
    }
    if (node.variant === 'solparq') {
      const tile = b.mk('#FDE68A', { roughness: 0.6 });
      for (let i = 0; i < 3; i++) {
        const tp = b.pivot(NODE_YAW + 0.18 * (i - 1));
        b.add(box(0.46, 0.05, 0.34), tile, 0, bodyTop + 0.03 + i * 0.065, 0, tp);
      }
      top = bodyTop + 0.2;
    }
    spec.height = top;
    return spec;
  }

  function buildTable(node, b) {
    const isBase = node.variant !== 'view';
    const color = new THREE.Color(isBase ? PALETTE.yellow : '#2DD4BF');
    const ghost = Boolean(node.optional);
    const shards = node.shards > 1 ? 3 : 1;
    const replicas = node.replicas > 1 ? 2 : 1;
    const r = shards > 1 ? 0.2 : 0.4;
    const discH = 0.15;
    const gap = 0.035;
    const discs = 3;
    const discGeo = cyl(r, r, discH, 28);
    const front = [b.mk(color, { ghost, roughness: 0.4 }), b.mk(color.clone().lerp(new THREE.Color('#ffffff'), 0.35), { ghost, roughness: 0.4 })];
    const back = [
      b.mk(color.clone().lerp(new THREE.Color('#1E293B'), 0.25), { ghost, roughness: 0.4 }),
      b.mk(color.clone().lerp(new THREE.Color('#ffffff'), 0.15), { ghost, roughness: 0.4 }),
    ];
    const rim = ghost ? b.line(color.clone().lerp(new THREE.Color('#0F172A'), 0.2), 0.9) : null;
    for (let rep = replicas - 1; rep >= 0; rep--) {
      const mats = rep === 0 ? front : back;
      for (let s = 0; s < shards; s++) {
        const x = (s - (shards - 1) / 2) * 0.48;
        const z = rep === 0 ? 0 : -0.42;
        for (let k = 0; k < discs; k++) {
          const y = 0.02 + discH / 2 + k * (discH + gap);
          b.add(discGeo, k === discs - 1 ? mats[1] : mats[0], x, y, z);
          if (rim) {
            const l = new THREE.LineSegments(edgesOf(`cyl:${r}:${discH}`, discGeo), rim);
            l.position.set(x, y, z);
            b.group.add(l);
          }
        }
      }
    }
    const height = 0.02 + discs * (discH + gap);
    return { radius: shards > 1 ? 0.78 : 0.48, height, inH: height * 0.55, outH: height };
  }

  function buildCluster(node, b) {
    const shards = node.shards > 1 ? 3 : 1;
    const replicas = node.replicas > 1 ? 2 : 1;
    const bw = shards > 1 ? 0.5 : 0.9;
    const bh = shards > 1 ? 0.82 : 0.98;
    const body = b.mk(theme.server, { roughness: 0.5 });
    const bodyBack = b.mk(new THREE.Color(theme.server).lerp(new THREE.Color('#000000'), 0.2), { roughness: 0.5 });
    const bar = b.mk(PALETTE.yellow, { emissive: PALETTE.yellow, emissiveIntensity: 0.18, roughness: 0.4 });
    const slot = b.mk('#0F172A', { roughness: 0.8 });
    for (let rep = replicas - 1; rep >= 0; rep--) {
      for (let s = 0; s < shards; s++) {
        const x = (s - (shards - 1) / 2) * 0.72;
        const z = rep === 0 ? 0 : -0.75;
        const p = b.pivot(NODE_YAW, x, z);
        b.add(rbox(bw, bh, bw, 0.05), rep === 0 ? body : bodyBack, 0, bh / 2, 0, p);
        // ClickHouse's mark: four bars, the last one short.
        const barW = bw * 0.12;
        for (let i = 0; i < 4; i++) {
          const len = i === 3 ? bw * 0.3 : bw * 0.62;
          b.add(box(barW, 0.05, len), bar, (i - 1.5) * barW * 1.7, bh + 0.025, i === 3 ? 0 : 0, p);
        }
        for (let i = 0; i < 2; i++) b.add(box(bw * 0.6, 0.03, 0.02), slot, 0, bh * (0.35 + i * 0.2), bw / 2 + 0.005, p);
      }
    }
    return { radius: shards > 1 ? 1.25 : 0.75, height: bh + 0.06, inH: bh * 0.6, outH: bh };
  }

  function buildCoordinator(b) {
    b.add(cyl(0.42, 0.44, 0.4, 6), b.mk(theme.shell, { roughness: 0.55 }), 0, 0.2, 0);
    b.add(cyl(0.3, 0.3, 0.05, 6), b.mk('#CBD5E1', { roughness: 0.4 }), 0, 0.425, 0);
    b.add(sphere(0.09), b.mk(PALETTE.yellow, { emissive: PALETTE.yellow, emissiveIntensity: 0.4 }), 0, 0.5, 0);
    return { radius: 0.48, height: 0.62, inH: 0.25, outH: 0.4 };
  }

  function buildMemory(b) {
    const p = b.pivot();
    b.add(rbox(1.0, 0.14, 1.0, 0.03), b.mk('#1E1B4B', { roughness: 0.6 }), 0, 0.16, 0, p);
    b.add(rbox(0.5, 0.06, 0.5, 0.02), b.mk(PALETTE.purple, { emissive: PALETTE.purple, emissiveIntensity: 0.25, roughness: 0.35 }), 0, 0.26, 0, p);
    const pin = b.mk(theme.metal, { metalness: 0.5, roughness: 0.35 });
    for (let i = 0; i < 6; i++) {
      const o = -0.375 + i * 0.15;
      b.add(box(0.07, 0.04, 0.16), pin, o, 0.1, 0.56, p);
      b.add(box(0.07, 0.04, 0.16), pin, o, 0.1, -0.56, p);
      b.add(box(0.16, 0.04, 0.07), pin, 0.56, 0.1, o, p);
      b.add(box(0.16, 0.04, 0.07), pin, -0.56, 0.1, o, p);
    }
    return { radius: 0.8, height: 0.32, inH: 0.2, outH: 0.28 };
  }

  function buildLocalDb(b) {
    const body = b.mk(PALETTE.green, { roughness: 0.4 });
    const band = b.mk(new THREE.Color(PALETTE.green).lerp(new THREE.Color('#ffffff'), 0.45), { roughness: 0.4 });
    b.add(cyl(0.4, 0.4, 0.66, 28), body, 0, 0.35, 0);
    b.add(cyl(0.41, 0.41, 0.05, 28), band, 0, 0.3, 0);
    b.add(cyl(0.41, 0.41, 0.05, 28), band, 0, 0.5, 0);
    const loop = b.mk('#5EEAD4', { emissive: '#5EEAD4', emissiveIntensity: 0.25 });
    const ringMesh = b.add(torus(0.56, 0.03, 8, 40), loop, 0, 0.42, 0);
    ringMesh.rotation.x = Math.PI / 2;
    const arrow = b.add(cone(0.07, 0.16, 10), loop, 0.56, 0.42, 0);
    arrow.rotation.x = Math.PI / 2;
    return { radius: 0.6, height: 0.72, inH: 0.4, outH: 0.68 };
  }

  function buildClients(node, b) {
    const glow = node.variant === 'grpc' ? '#A78BFA' : '#60A5FA';
    const frame = b.mk(theme.shell, { roughness: 0.6 });
    const screen = b.mk(glow, { emissive: glow, emissiveIntensity: 0.35, roughness: 0.3 });
    const spots = [
      [-0.5, 0.12, 0.35],
      [0, -0.2, 0],
      [0.5, 0.12, -0.35],
    ];
    for (const [x, z, yaw] of spots) {
      const p = b.pivot(yaw, x, z);
      b.add(box(0.2, 0.03, 0.14), frame, 0, 0.015, 0, p);
      b.add(box(0.04, 0.2, 0.04), frame, 0, 0.13, -0.02, p);
      b.add(box(0.48, 0.32, 0.04), frame, 0, 0.38, 0, p);
      b.add(box(0.42, 0.26, 0.01), screen, 0, 0.38, 0.022, p);
    }
    return { radius: 0.85, height: 0.6, inH: 0.38, outH: 0.5 };
  }

  function buildStore(node, b) {
    if (node.variant === 's3') {
      const shell = b.mk(PALETTE.orange, { side: THREE.DoubleSide, roughness: 0.45 });
      b.add(cyl(0.6, 0.44, 0.78, 32, true), shell, 0, 0.41, 0);
      b.add(cyl(0.44, 0.44, 0.02, 32), b.mk('#B45309'), 0, 0.03, 0);
      const rim = b.add(torus(0.6, 0.035, 8, 40), b.mk('#FBBF24', { roughness: 0.35 }), 0, 0.8, 0);
      rim.rotation.x = Math.PI / 2;
      return { radius: 0.66, height: 0.86, inH: 0.6, outH: 0.8 };
    }
    const p = b.pivot();
    b.add(rbox(1.2, 0.16, 1.0, 0.04), b.mk(PALETTE.orange, { roughness: 0.5 }), 0, 0.08, 0, p);
    const platter = b.mk(theme.metal, { metalness: 0.45, roughness: 0.3 });
    const label = b.mk('#FDE68A', { roughness: 0.6 });
    for (let i = 0; i < 3; i++) {
      b.add(cyl(0.42, 0.42, 0.05, 32), platter, 0, 0.22 + i * 0.12, 0);
      b.add(cyl(0.14, 0.14, 0.055, 20), label, 0, 0.22 + i * 0.12, 0);
    }
    b.add(cyl(0.04, 0.04, 0.42, 10), b.mk(theme.dark), 0, 0.36, 0);
    return { radius: 0.75, height: 0.6, inH: 0.35, outH: 0.5 };
  }

  function buildFallback(b) {
    b.add(rbox(0.8, 0.6, 0.8, 0.06), b.mk(PALETTE.slate), 0, 0.3, 0);
    return { radius: 0.6, height: 0.6, inH: 0.3, outH: 0.6 };
  }

  function buildBody(node, b) {
    switch (node.kind) {
      case 'network':
        return buildNetwork(b);
      case 'endpoint':
        return buildEndpoint(node, b);
      case 'process':
        return buildProcess(node, b);
      case 'table':
        return buildTable(node, b);
      case 'cluster':
        return buildCluster(node, b);
      case 'coordinator':
        return buildCoordinator(b);
      case 'memory':
        return buildMemory(b);
      case 'localdb':
        return buildLocalDb(b);
      case 'clients':
        return buildClients(node, b);
      case 'store':
        return buildStore(node, b);
      default:
        return buildFallback(b);
    }
  }

  const meshKey = (n) => [n.kind, n.variant, n.shards, n.replicas, n.optional ? 1 : 0, n.buffer ? 1 : 0].join('|');

  // --- Nodes ---------------------------------------------------------------
  function createNodeView(node) {
    const group = new THREE.Group();
    const [u0, v0] = node.pos ?? [0, 0];
    group.position.set(u0, 0, v0);
    layout.add(group);

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'node-label';
    button.dataset.nodeId = node.id;
    // Hidden until the first layout pass places it (see setLabelHidden).
    button.style.clipPath = 'inset(50%)';
    button.style.pointerEvents = 'none';
    const title = document.createElement('span');
    title.className = 'node-label__title';
    const sub = document.createElement('span');
    sub.className = 'node-label__sub';
    button.append(title, sub);
    const id = node.id;
    button.addEventListener('click', () => onSelect?.(id));
    button.addEventListener('pointerenter', () => setHoverNode(id));
    button.addEventListener('pointerleave', () => setHoverNode(null));
    // Tabbing to a label the layout had to hide reveals it.
    button.addEventListener('focus', () => {
      focusId = id;
      setHoverNode(id);
    });
    button.addEventListener('blur', () => {
      if (focusId === id) focusId = null;
      setHoverNode(null);
    });
    const label = new CSS2DObject(button);
    label.center.set(0.5, 1);
    group.add(label);
    // Attach now so the label can be measured before its first render.
    overlay.appendChild(button);

    const view = {
      id,
      data: node,
      group,
      body: null,
      mats: [],
      spec: null,
      key: '',
      label,
      button,
      title,
      sub,
      appear: reducedMotion ? 1 : 0,
      leaving: false,
      posFrom: group.position.clone(),
      posTo: group.position.clone(),
      posT: 1,
      buf: { count: 0, firstAt: 0 },
      labelSide: null,
      compact: false,
    };
    nodeViews.set(id, view);
    rebuildNodeBody(view);
    applyNodeData(view, node);
    return view;
  }

  function rebuildNodeBody(view) {
    if (view.body) {
      view.group.remove(view.body);
      for (const m of view.mats) m.dispose();
      view.mats = [];
    }
    const body = new THREE.Group();
    const b = makeBuilder(body, view.mats);
    const spec = buildBody(view.data, b);
    const shadowMat = new THREE.MeshBasicMaterial({ color: '#0F172A', alphaMap: shadowTex, transparent: true, depthWrite: false, opacity: theme.shadow });
    shadowMat.userData.base = shadowMat.color.clone();
    shadowMat.userData.baseOpacity = theme.shadow;
    shadowMat.userData.isShadow = true;
    view.mats.push(shadowMat);
    const shadow = new THREE.Mesh(flatPlane(), shadowMat);
    shadow.scale.setScalar(spec.radius * 2.9);
    shadow.position.y = 0.006;
    shadow.renderOrder = -1;
    body.add(shadow);
    body.traverse((o) => {
      o.userData.nodeId = view.id;
    });
    body.rotation.y = poseYaw();
    view.group.add(body);
    view.body = body;
    view.spec = spec;
    view.key = meshKey(view.data);
    view.label.position.set(0, spec.height + 0.12, 0);
    if (selectedId === view.id) attachSelectRing(view);
    applyNodeLook(view);
  }

  function applyNodeData(view, node) {
    const prev = view.data;
    view.data = node;
    if (view.key !== meshKey(node)) rebuildNodeBody(view);
    view.title.textContent = node.label;
    view.sub.textContent = node.sublabel || '';
    view.sub.hidden = !node.sublabel;
    const cls = view.button.classList;
    cls.toggle('is-optional', Boolean(node.optional));
    cls.toggle('is-selected', selectedId === node.id);
    for (const s of ['ok', 'warn', 'info']) cls.toggle(`status-${s}`, node.status === s);
    view.button.setAttribute('aria-label', node.sublabel ? `${node.label}: ${node.sublabel}` : node.label);
    if (view.spec.statusMat && prev.status !== node.status) {
      const c = STATUS_COLOR[node.status] ?? PALETTE.slate;
      view.spec.statusMat.userData.base.set(c);
      view.spec.statusMat.userData.baseEmissive.set(c);
    }
    const [u, v] = node.pos ?? [0, 0];
    if (view.posTo.x !== u || view.posTo.z !== v) {
      view.posFrom.copy(view.group.position);
      view.posTo.set(u, 0, v);
      view.posT = reducedMotion ? 1 : 0;
      if (reducedMotion) view.group.position.copy(view.posTo);
    }
    if (!node.buffer) view.buf.count = 0;
    applyNodeLook(view);
  }

  function applyNodeLook(view) {
    const hover = hoverNodeId === view.id;
    const fade = easeOut(view.appear);
    for (const m of view.mats) {
      const { base, baseOpacity } = m.userData;
      m.color.copy(base);
      setOpacity(m, baseOpacity * fade);
      if (m.emissive) {
        m.emissive.copy(m.userData.baseEmissive);
        if (hover) m.emissive.lerp(base, 0.35);
      }
    }
    const s = 0.82 + 0.18 * fade;
    view.group.scale.setScalar(s);
    view.button.style.opacity = view.appear < 1 ? String(fade) : '';
  }

  function setOpacity(m, opacity) {
    m.opacity = opacity;
    const transparent = opacity < 0.999 || Boolean(m.userData.isShadow) || Boolean(m.userData.isLine);
    if (m.transparent !== transparent) {
      m.transparent = transparent;
      m.needsUpdate = true;
    }
  }

  function destroyNodeView(view) {
    if (selectRing.parent === view.group) detachSelectRing();
    if (focusId === view.id) focusId = null;
    if (hoverNodeId === view.id) {
      hoverNodeId = null;
      canvas.style.cursor = '';
    }
    layout.remove(view.group);
    view.group.remove(view.label);
    view.label.element.remove();
    for (const m of view.mats) m.dispose();
    view.mats = [];
    nodeViews.delete(view.id);
  }

  // --- Zones ---------------------------------------------------------------
  function createZoneView(zone) {
    const mat = new THREE.MeshStandardMaterial({ roughness: 0.92, metalness: 0, transparent: true, opacity: 0 });
    const mesh = new THREE.Mesh(undefined, mat);
    mesh.renderOrder = -2;
    layout.add(mesh);
    const el = document.createElement('div');
    el.className = 'zone-label';
    const label = new CSS2DObject(el);
    label.center.set(0, 1);
    layout.add(label);
    overlay.appendChild(el);
    // Hairline outline on the slab's top face, echoing the bordered cells on
    // solana.com/data.
    const lineMat = new THREE.LineBasicMaterial({ color: '#ECE4FD', transparent: true, opacity: 0, depthWrite: false });
    const outline = new THREE.LineLoop(undefined, lineMat);
    outline.renderOrder = -1;
    layout.add(outline);
    const view = { id: zone.id, data: zone, mesh, mat, outline, lineMat, label, el, appear: reducedMotion ? 1 : 0, leaving: false };
    zoneViews.set(zone.id, view);
    placeZoneLabel(view);
    applyZoneData(view, zone);
    return view;
  }

  // Zone names sit just above the slab's far edge at its left end, like a tab
  // on a folder, where no node stands.
  function placeZoneLabel(view) {
    const [u0, v0, , v1] = view.data.rect;
    if (pose === 'portrait') view.label.position.set(u0, 0, v1 - 0.2);
    else view.label.position.set(u0 + 0.2, 0, v0);
  }

  // A zone's rect can change between updates (the Upstream slab hugs the
  // endpoints in use). Geometries are cached by size, so this never leaks.
  function applyZoneData(view, zone) {
    view.data = zone;
    const [u0, v0, u1, v1] = zone.rect;
    const w = u1 - u0;
    const d = v1 - v0;
    view.mesh.geometry = geo(`zone:${w.toFixed(2)}:${d.toFixed(2)}`, () => new RoundedBoxGeometry(w, ZONE_H, d, 3, 0.12));
    view.mesh.position.set((u0 + u1) / 2, -ZONE_H / 2, (v0 + v1) / 2);
    // Inset so the square outline stays inside the slab's rounded corners.
    const hw = w / 2 - 0.06;
    const hd = d / 2 - 0.06;
    view.outline.geometry = geo(`zoneline:${w.toFixed(2)}:${d.toFixed(2)}`, () =>
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(-hw, 0, -hd),
        new THREE.Vector3(hw, 0, -hd),
        new THREE.Vector3(hw, 0, hd),
        new THREE.Vector3(-hw, 0, hd),
      ]),
    );
    view.outline.position.set((u0 + u1) / 2, 0.004, (v0 + v1) / 2);
    placeZoneLabel(view);
    view.el.textContent = zone.label;
    applyZoneLook(view);
  }

  function applyZoneLook(view) {
    const tint = theme.zoneTint * (theme.tintScale[view.id] ?? 1);
    view.mat.color.set(theme.platform).lerp(new THREE.Color(ZONE_TINT[view.id] ?? PALETTE.slate), tint);
    const fade = easeOut(view.appear);
    setOpacity(view.mat, fade);
    view.lineMat.opacity = 0.18 * fade;
    view.mesh.scale.y = 0.2 + 0.8 * fade;
    view.el.style.opacity = view.appear < 1 ? String(fade) : '';
  }

  function destroyZoneView(view) {
    layout.remove(view.mesh, view.outline);
    layout.remove(view.label);
    view.el.remove();
    view.mat.dispose();
    view.lineMat.dispose();
    zoneViews.delete(view.id);
  }

  // --- Edges ---------------------------------------------------------------
  function edgeTint(data) {
    if (data.style === 'control') return new THREE.Color(PALETTE.slate);
    const accent = data.style === 'read' ? PALETTE.blue : PARTICLE_COLOR[data.particle] ?? PALETTE.slate;
    return new THREE.Color(theme.edgeNeutral).lerp(new THREE.Color(accent), theme.edgeMix);
  }

  function edgeGeoKey(data, from, to) {
    const a = from.posTo;
    const b = to.posTo;
    return [pose, data.style, a.x, a.z, from.spec.outH, to.spec.inH, b.x, b.z, to.spec.radius].join('|');
  }

  function createEdgeView(data, from, to) {
    const mat = new THREE.MeshBasicMaterial({ transparent: true, depthWrite: false });
    const arrowMat = new THREE.MeshBasicMaterial({ transparent: true, depthWrite: false });
    const view = {
      id: data.id,
      data,
      mat,
      arrowMat,
      mesh: null,
      arrow: null,
      curve: null,
      length: 1,
      samples: new Float32Array((EDGE_SAMPLES + 1) * 3),
      key: '',
      appear: reducedMotion ? 1 : 0,
      leaving: false,
      removed: false,
      em: { wait: 0, cycle: 0, left: 0, next: 0, depth: 0 },
      emitJson: JSON.stringify(data.emit ?? null),
    };
    edgeViews.set(data.id, view);
    buildEdgeGeometry(view, from, to);
    applyEdgeData(view, data);
    return view;
  }

  function buildEdgeGeometry(view, from, to) {
    if (view.mesh) {
      layout.remove(view.mesh, view.arrow);
      view.mesh.geometry.dispose();
    }
    const data = view.data;
    const a = new THREE.Vector3(from.posTo.x, from.spec.outH, from.posTo.z);
    const b = new THREE.Vector3(to.posTo.x, to.spec.inH, to.posTo.z);
    const flat = Math.hypot(b.x - a.x, b.z - a.z);
    const lift = clamp(flat * 0.16, 0.6, 2.2);
    const mid = a.clone().lerp(b, 0.5);
    mid.y = Math.max(a.y, b.y) + lift;
    // The lift reads as screen-up, which is invisible on an edge that already
    // runs screen-vertical; bow those sideways so they clear what they pass.
    if (flat > 0) {
      const vertical = Math.abs(pose === 'portrait' ? b.x - a.x : b.z - a.z) / flat;
      const bow = 0.14 * flat * THREE.MathUtils.smoothstep(vertical, 0.55, 0.95);
      if (pose === 'portrait') mid.z -= bow;
      else mid.x += bow;
    }
    const curve = new THREE.CatmullRomCurve3([a, mid, b], false, 'centripetal');
    const length = curve.getLength();
    const segments = Math.max(24, Math.round(length * 8));
    const control = data.style === 'control';
    const g = new THREE.TubeGeometry(curve, segments, control ? 0.026 : 0.035, 6, false);
    if (control) {
      // Stretch the along-curve UV so the dash texture repeats every ~0.45u.
      const uv = g.attributes.uv;
      const repeats = length / 0.45;
      for (let i = 0; i < uv.count; i++) uv.setX(i, uv.getX(i) * repeats);
      uv.needsUpdate = true;
    }
    const mesh = new THREE.Mesh(g, view.mat);
    mesh.renderOrder = 1;
    mesh.userData.edgeId = view.id;

    // Arrowhead sits where the curve enters the target's footprint.
    const r = to.spec.radius + 0.12;
    let tArrow = 0.92;
    for (let t = 1; t > 0.5; t -= 0.005) {
      curve.getPointAt(t, tmpPos);
      if (Math.hypot(tmpPos.x - b.x, tmpPos.z - b.z) > r) {
        tArrow = t;
        break;
      }
    }
    const arrow = new THREE.Mesh(cone(0.085, 0.24, 12), view.arrowMat);
    curve.getPointAt(tArrow, arrow.position);
    curve.getTangentAt(tArrow, tmpVec);
    arrow.quaternion.setFromUnitVectors(UP, tmpVec.normalize());
    arrow.renderOrder = 1;

    layout.add(mesh, arrow);
    for (let i = 0; i <= EDGE_SAMPLES; i++) {
      curve.getPointAt(i / EDGE_SAMPLES, tmpPos);
      layout.localToWorld(tmpPos);
      tmpPos.toArray(view.samples, i * 3);
    }
    view.mesh = mesh;
    view.arrow = arrow;
    view.curve = curve;
    view.length = Math.max(length, 0.01);
    view.indexCount = g.index ? g.index.count : g.attributes.position.count;
    view.key = edgeGeoKey(data, from, to);
  }

  function applyEdgeData(view, data) {
    view.data = data;
    const alphaMap = data.style === 'control' ? dashTex : null;
    if (view.mat.alphaMap !== alphaMap) {
      view.mat.alphaMap = alphaMap;
      view.mat.alphaTest = alphaMap ? 0.02 : 0;
      view.mat.needsUpdate = true;
    }
    // Compare against the stored spec: update() may already have replaced
    // view.data before calling this.
    const emitJson = JSON.stringify(data.emit ?? null);
    if (emitJson !== view.emitJson) {
      view.emitJson = emitJson;
      resetEmitter(view);
    }
    applyEdgeLook(view);
  }

  function resetEmitter(view) {
    const em = view.em;
    const emit = view.data.emit;
    em.wait = 0;
    em.left = 0;
    em.next = simTime;
    em.cycle = emit && emit.type === 'burst' ? Math.floor((simTime - (emit.offset || 0)) / Math.max(emit.every, 0.2)) - 1 : 0;
  }

  function applyEdgeLook(view) {
    const data = view.data;
    const hover = hoverEdgeId === view.id;
    const tint = edgeTint(data);
    const base = data.style === 'control' ? 0.85 : data.style === 'read' ? 0.7 : 0.72;
    const fade = easeOut(view.appear);
    const opacity = (hover ? 1 : base) * (data.conditional ? 0.45 : 1) * fade;
    view.mat.color.copy(tint);
    view.arrowMat.color.copy(tint);
    if (hover) {
      view.mat.color.lerp(new THREE.Color('#ffffff'), 0.2);
      view.arrowMat.color.copy(view.mat.color);
    }
    view.mat.opacity = opacity;
    view.arrowMat.opacity = Math.min(1, opacity * 1.15);
    const drawn = Math.floor((view.indexCount * fade) / 3) * 3;
    view.mesh.geometry.setDrawRange(0, drawn);
    view.arrow.visible = fade > 0.85;
  }

  function destroyEdgeView(view) {
    view.removed = true;
    recycleParticlesOn(view);
    layout.remove(view.mesh, view.arrow);
    view.mesh.geometry.dispose();
    view.mat.dispose();
    view.arrowMat.dispose();
    if (hoverEdgeId === view.id) setHoverEdge(null);
    edgeViews.delete(view.id);
  }

  function recycleParticlesOn(view) {
    for (let i = cubeCount - 1; i >= 0; i--) if (cubes[i].edge === view) freeCube(i);
    for (let i = sphereCount - 1; i >= 0; i--) {
      const j = spheres[i];
      for (let k = 0; k < j.n; k++) {
        if (j.legEdge[k] === view) {
          freeSphere(i);
          break;
        }
      }
    }
  }

  // --- Reconcile -----------------------------------------------------------
  function update(topo) {
    if (disposed || !topo) return;
    const firstUpdate = !topology;
    topology = topo;
    refitToZones(topo.zones ?? EMPTY, !firstUpdate);

    const zoneIds = new Set();
    for (const zone of topo.zones ?? EMPTY) {
      zoneIds.add(zone.id);
      const view = zoneViews.get(zone.id);
      if (!view) createZoneView(zone);
      else {
        view.leaving = false;
        applyZoneData(view, zone);
      }
    }
    for (const view of zoneViews.values()) if (!zoneIds.has(view.id)) view.leaving = true;

    const nodeIds = new Set();
    for (const node of topo.nodes ?? EMPTY) {
      nodeIds.add(node.id);
      const view = nodeViews.get(node.id);
      if (!view) createNodeView(node);
      else {
        view.leaving = false;
        applyNodeData(view, node);
      }
    }
    for (const view of nodeViews.values()) {
      if (!nodeIds.has(view.id)) {
        view.leaving = true;
        view.buf.count = 0;
      }
    }

    const edgeIds = new Set();
    for (const data of topo.edges ?? EMPTY) {
      const from = nodeViews.get(data.from);
      const to = nodeViews.get(data.to);
      if (!from || !to || from.leaving || to.leaving) continue;
      edgeIds.add(data.id);
      let view = edgeViews.get(data.id);
      if (!view) {
        view = createEdgeView(data, from, to);
        resetEmitter(view);
      } else {
        view.leaving = false;
        if (view.key !== edgeGeoKey(data, from, to) || view.data.style !== data.style) {
          view.data = data;
          buildEdgeGeometry(view, from, to);
          // The tube is built at the endpoints' final positions; fade it back
          // in while a node slides there so it never points at empty space.
          if (!reducedMotion && (from.posT < 1 || to.posT < 1)) view.appear = 0;
        }
        applyEdgeData(view, data);
      }
    }
    for (const view of edgeViews.values()) {
      if (!edgeIds.has(view.id) && !view.leaving) {
        view.leaving = true;
        // Particles on removed edges are recycled immediately; the tube fades.
        recycleParticlesOn(view);
      }
    }

    outEdges = new Map();
    for (const view of edgeViews.values()) {
      if (view.leaving) continue;
      const list = outEdges.get(view.data.from);
      if (list) list.push(view);
      else outEdges.set(view.data.from, [view]);
    }

    if (!topo.animate?.data) {
      for (const view of nodeViews.values()) view.buf.count = 0;
      for (const view of edgeViews.values()) if (view.data.channel === 'data') view.em.left = 0;
    }
    if (!topo.animate?.serve) {
      for (let i = sphereCount - 1; i >= 0; i--) freeSphere(i);
    }

    if (reducedMotion) {
      for (const view of [...nodeViews.values()]) if (view.leaving) destroyNodeView(view);
      for (const view of [...edgeViews.values()]) if (view.leaving) destroyEdgeView(view);
      for (const view of [...zoneViews.values()]) if (view.leaving) destroyZoneView(view);
    }
    if (selectedId && (!nodeViews.has(selectedId) || nodeViews.get(selectedId).leaving)) setSelected(null);
    if (hoverNodeId && !nodeViews.has(hoverNodeId)) setHoverNode(null);
    rebuildPickables();
    // Recycling reorders the pools; rewrite now so a paused scene does not
    // keep drawing particles from edges that are gone.
    writeInstances();
    labelsDirty = true;
    invalidate();
  }

  function rebuildPickables() {
    pickables = [];
    for (const view of nodeViews.values()) {
      if (view.leaving) continue;
      // The contact shadow is wider than the node; picking it would select
      // nodes from clicks on empty ground.
      view.body.traverse((o) => {
        if (o.isMesh && !o.material.userData.isShadow) pickables.push(o);
      });
    }
  }

  // --- Tweens --------------------------------------------------------------
  function stepTweens(dt) {
    let active = false;
    const step = dt / FADE_S;
    for (const view of nodeViews.values()) {
      if (view.leaving) {
        view.appear = Math.max(0, view.appear - step);
        if (view.appear <= 0) {
          destroyNodeView(view);
          continue;
        }
        applyNodeLook(view);
        active = true;
      } else if (view.appear < 1) {
        view.appear = Math.min(1, view.appear + step);
        applyNodeLook(view);
        active = true;
      }
      if (view.posT < 1) {
        view.posT = Math.min(1, view.posT + step);
        view.group.position.lerpVectors(view.posFrom, view.posTo, easeOut(view.posT));
        active = true;
      }
    }
    for (const view of edgeViews.values()) {
      if (view.leaving) {
        view.appear = Math.max(0, view.appear - step);
        if (view.appear <= 0) {
          destroyEdgeView(view);
          continue;
        }
        applyEdgeLook(view);
        active = true;
      } else if (view.appear < 1) {
        view.appear = Math.min(1, view.appear + step * 0.8);
        applyEdgeLook(view);
        active = true;
      }
    }
    for (const view of zoneViews.values()) {
      if (view.leaving) {
        view.appear = Math.max(0, view.appear - step);
        if (view.appear <= 0) {
          destroyZoneView(view);
          continue;
        }
        applyZoneLook(view);
        active = true;
      } else if (view.appear < 1) {
        view.appear = Math.min(1, view.appear + step);
        applyZoneLook(view);
        active = true;
      }
    }
    if (stepCameraTween(dt)) active = true;
    return active;
  }

  // --- Particles -----------------------------------------------------------
  const gate = (view) => {
    const channel = view.data.channel;
    if (channel === 'data') return Boolean(topology?.animate?.data);
    if (channel === 'serve') return Boolean(topology?.animate?.serve);
    return false;
  };

  function spawnCube(view, depth) {
    if (cubeCount >= MAX_CUBES || !view.data.particle) return;
    const p = cubes[cubeCount++];
    p.edge = view;
    p.t = 0;
    p.type = PARTICLE_SIZE[view.data.particle] ? view.data.particle : 'block';
    p.depth = depth;
    p.spin = Math.random() * Math.PI;
    p.spinRate = (Math.random() < 0.5 ? -1 : 1) * (1 + Math.random() * 1.5);
  }

  function freeCube(i) {
    const last = --cubeCount;
    const p = cubes[i];
    p.edge = null;
    cubes[i] = cubes[last];
    cubes[last] = p;
  }

  function freeSphere(i) {
    const last = --sphereCount;
    const j = spheres[i];
    j.legEdge.fill(null);
    j.n = 0;
    spheres[i] = spheres[last];
    spheres[last] = j;
  }

  function stepEmitter(view) {
    const emit = view.data.emit;
    const em = view.em;
    switch (emit.type) {
      case 'stream': {
        em.wait -= lastDt;
        if (em.wait <= 0) {
          if (!view.data.conditional || Math.random() < 0.5) spawnCube(view, 0);
          em.wait = Math.max(0, em.wait + jitter(1 / Math.max(emit.rate || 1, 0.05)));
        }
        break;
      }
      case 'burst': {
        const every = Math.max(emit.every || 1, 0.2);
        const offset = emit.offset || 0;
        const cycle = Math.floor((simTime - offset) / every);
        if (cycle > em.cycle && simTime >= offset) {
          em.cycle = cycle;
          em.left = view.data.conditional ? Math.ceil((emit.count || 1) / 2) : emit.count || 1;
          em.next = simTime;
        }
        if (em.left > 0 && simTime >= em.next) {
          spawnCube(view, 0);
          em.left--;
          em.next += BURST_GAP_S;
        }
        break;
      }
      case 'flush': {
        if (em.left > 0 && simTime >= em.next) {
          spawnCube(view, 0);
          em.left--;
          em.next += 0.1;
        }
        break;
      }
      case 'relay': {
        // Counted relays (emit.count > 1) queue a burst in arrive().
        if (em.left > 0 && simTime >= em.next) {
          spawnCube(view, em.depth || 1);
          em.left--;
          em.next += BURST_GAP_S;
        }
        break;
      }
      default:
        break;
    }
  }

  function flush(view) {
    const size = view.data.buffer.size;
    const per = view.buf.count >= size ? 3 : 2;
    view.buf.count = 0;
    for (const out of outEdges.get(view.id) ?? EMPTY) {
      if (out.leaving || out.data.emit?.type !== 'flush' || !gate(out)) continue;
      const order = out.data.emit.order;
      const delay = order === null || order === undefined ? Math.random() * 0.9 : order * FLUSH_STAGGER_S;
      if (out.em.left <= 0) out.em.next = simTime + delay;
      out.em.left += out.data.conditional ? 1 : per;
    }
  }

  function arrive(view, depth) {
    const to = nodeViews.get(view.data.to);
    if (!to || to.leaving) return;
    if (to.data.buffer && view.data.channel === 'data') {
      // Cubes already in flight when data animation is switched off must not
      // refill the hopper.
      if (!gate(view)) return;
      if (to.buf.count === 0) to.buf.firstAt = simTime;
      to.buf.count++;
      return;
    }
    if (depth >= MAX_RELAY_DEPTH) return;
    for (const out of outEdges.get(to.id) ?? EMPTY) {
      if (out.leaving || out.data.emit?.type !== 'relay' || !gate(out)) continue;
      if (out.data.conditional && Math.random() < 0.5) continue;
      const count = out.data.emit.count ?? 1;
      if (count <= 1) {
        spawnCube(out, depth + 1);
      } else {
        // One arrival triggers a whole burst (e.g. a query -> a table upload).
        if (out.em.left <= 0) out.em.next = simTime;
        out.em.left += count;
        out.em.depth = depth + 1;
      }
    }
  }

  function liveEdge(id) {
    const view = edgeViews.get(id);
    return view && !view.leaving ? view : null;
  }

  function spawnJourney() {
    const rp = topology?.readPath;
    if (!rp || sphereCount >= MAX_SPHERES) return;
    const entry = liveEdge(rp.entryEdge);
    if (!entry) return;
    let total = 0;
    for (const c of rp.classes) total += c.weight;
    let pick = Math.random() * total;
    let cls = rp.classes[rp.classes.length - 1];
    for (const c of rp.classes) {
      pick -= c.weight;
      if (pick <= 0) {
        cls = c;
        break;
      }
    }
    const j = spheres[sphereCount];
    j.n = 0;
    j.serveLeg = -1;
    const push = (edge, dir) => {
      if (j.n < MAX_LEGS) {
        j.legEdge[j.n] = edge;
        j.legDir[j.n] = dir;
        j.n++;
      }
    };
    push(entry, -1);
    for (const tier of rp.tiers) {
      const edge = liveEdge(tier.edge);
      if (!edge) continue;
      push(edge, -1);
      if (tier.serves.includes(cls.id)) {
        j.serveLeg = j.n;
        j.tierColor.set(TIER_COLOR[tier.node] ?? PALETTE.yellow);
        push(edge, 1);
        push(entry, 1);
        break;
      }
      push(edge, 1);
    }
    if (j.serveLeg < 0) {
      j.legEdge.fill(null);
      j.n = 0;
      return;
    }
    j.i = 0;
    j.t = 0;
    j.color.copy(pulseColor);
    sphereCount++;
  }

  let lastDt = 0;
  function stepSim(dt) {
    lastDt = dt;
    if (!topology) return;
    for (const view of edgeViews.values()) {
      if (view.leaving || !view.data.emit || !view.data.particle || !gate(view)) continue;
      stepEmitter(view);
    }
    for (const view of nodeViews.values()) {
      const buffer = view.data.buffer;
      if (!buffer || view.leaving) continue;
      const st = view.buf;
      if (st.count > 0 && (st.count >= buffer.size || simTime - st.firstAt >= buffer.maxWait)) flush(view);
    }
    if (topology.animate?.serve) {
      journeyWait -= dt;
      if (journeyWait <= 0) {
        spawnJourney();
        journeyWait = jitter(1 / JOURNEY_RATE);
      }
    }
    for (let i = cubeCount - 1; i >= 0; i--) {
      const p = cubes[i];
      p.t += (dt * p.edge.data.speed) / p.edge.length;
      p.spin += dt * p.spinRate;
      if (p.t >= 1) {
        const edge = p.edge;
        const depth = p.depth;
        freeCube(i);
        arrive(edge, depth);
      }
    }
    for (let i = sphereCount - 1; i >= 0; i--) {
      const j = spheres[i];
      const edge = j.legEdge[j.i];
      j.t += (dt * JOURNEY_SPEED) / edge.length;
      if (j.t >= 1) {
        j.t = 0;
        j.i++;
        if (j.i === j.serveLeg) j.color.copy(j.tierColor);
        if (j.i >= j.n) freeSphere(i);
      }
    }
    writeInstances();
  }

  function writeInstances() {
    for (let i = 0; i < cubeCount; i++) {
      const p = cubes[i];
      p.edge.curve.getPointAt(Math.min(p.t, 1), tmpPos);
      const [sx, sy, sz] = PARTICLE_SIZE[p.type];
      const ramp = 0.3 + 0.7 * clamp(Math.min(p.t, 1 - p.t) / 0.05, 0, 1);
      tmpScale.set(sx * ramp, sy * ramp, sz * ramp);
      if (p.type === 'parquet' || p.type === 'meta') tmpEuler.set(0, p.spin * 0.5, 0);
      else tmpEuler.set(p.spin * 0.6, p.spin, 0);
      tmpQuat.setFromEuler(tmpEuler);
      tmpMat.compose(tmpPos, tmpQuat, tmpScale);
      cubeMesh.setMatrixAt(i, tmpMat);
      cubeMesh.setColorAt(i, particleColors[p.type]);
    }
    cubeMesh.count = cubeCount;
    cubeMesh.instanceMatrix.needsUpdate = true;
    cubeMesh.instanceColor.needsUpdate = true;

    tmpQuat.identity();
    for (let i = 0; i < sphereCount; i++) {
      const j = spheres[i];
      const edge = j.legEdge[j.i];
      const t = j.legDir[j.i] > 0 ? j.t : 1 - j.t;
      edge.curve.getPointAt(clamp(t, 0, 1), tmpPos);
      tmpScale.setScalar(0.12);
      tmpMat.compose(tmpPos, tmpQuat, tmpScale);
      sphereMesh.setMatrixAt(i, tmpMat);
      sphereMesh.setColorAt(i, j.color);
    }
    sphereMesh.count = sphereCount;
    sphereMesh.instanceMatrix.needsUpdate = true;
    sphereMesh.instanceColor.needsUpdate = true;

    writeHopper();
  }

  // Hopper slots in node-local space: a 2x2 grid per layer, turned to match
  // the machine body.
  const HOPPER_SLOTS = [];
  for (let layer = 0; layer < 3; layer++) {
    for (const [a, c] of [
      [-1, -1],
      [1, -1],
      [-1, 1],
      [1, 1],
    ]) {
      const x = a * 0.085;
      const z = c * 0.085;
      const cos = Math.cos(NODE_YAW);
      const sin = Math.sin(NODE_YAW);
      HOPPER_SLOTS.push([x * cos + z * sin, layer * 0.15, -x * sin + z * cos]);
    }
  }

  function writeHopper() {
    let n = 0;
    for (const view of nodeViews.values()) {
      if (!view.spec?.hopper || view.leaving || view.buf.count <= 0) continue;
      const count = Math.min(view.buf.count, MAX_HOPPER - n);
      const g = view.group.position;
      const s = view.group.scale.x;
      for (let k = 0; k < count; k++) {
        const [x, y, z] = HOPPER_SLOTS[k];
        tmpPos.set(g.x + x * s, (view.spec.hopper.y + 0.08 + y) * s, g.z + z * s);
        tmpEuler.set(0, NODE_YAW, 0);
        tmpQuat.setFromEuler(tmpEuler);
        tmpScale.setScalar(0.15 * s);
        tmpMat.compose(tmpPos, tmpQuat, tmpScale);
        hopperMesh.setMatrixAt(n++, tmpMat);
      }
    }
    hopperMesh.count = n;
    hopperMesh.instanceMatrix.needsUpdate = true;
  }

  // --- Label placement -----------------------------------------------------
  // Greedy screen-space placement: labels are taken in priority order and
  // each tries the sides listed in LABEL_SIDES. A side that would cover an
  // already placed label (or leave the canvas) is rejected; covering another
  // node's mesh is only penalised. When no side is good, the label drops its
  // sublabel and tries again; if that fails too it is hidden until the user
  // zooms in, hovers or selects the node.
  const camRight = new THREE.Vector3();
  const camUp = new THREE.Vector3();
  const layoutInverse = new THREE.Quaternion().setFromAxisAngle(UP, -LAYOUT_YAW);
  const placed = [];
  const candidate = { side: null, cost: 0, ax: 0, ay: 0, rect: [0, 0, 0, 0] };
  const fullChoice = { side: null, cost: 0, ax: 0, ay: 0, rect: [0, 0, 0, 0] };
  const scratchRect = [0, 0, 0, 0];
  let labelsDirty = true;

  function toScreen(x, y, z, out) {
    tmpPos.set(x, y, z);
    layout.localToWorld(tmpPos).project(camera);
    out[0] = ((tmpPos.x + 1) / 2) * size.width;
    out[1] = ((1 - tmpPos.y) / 2) * size.height;
    return out;
  }

  // Hover or keyboard focus promotes a label only if it was hidden when that
  // hover/focus began (revealId), and it stays promoted until pointer and focus
  // have both left. Promoting an already visible label could move it out from
  // under the cursor and flip hover off and on.
  let revealId = null;
  const revealsOnHover = (view) => view.id === revealId && (view.id === hoverNodeId || view.id === focusId);

  // Labels the layout cannot fit are clipped away rather than made invisible,
  // so they stay in the tab order and keyboard users can still reach them.
  function setLabelHidden(view, hidden) {
    view.button.style.clipPath = hidden ? 'inset(50%)' : '';
    view.button.style.pointerEvents = hidden ? 'none' : 'auto';
  }

  function labelPriority(view) {
    if (view.id === selectedId) return -2;
    if (revealsOnHover(view)) return -1;
    return LABEL_PRIORITY[view.data.kind] ?? 5;
  }

  const overlaps = (a, b) => a[0] < b[2] && a[2] > b[0] && a[1] < b[3] && a[3] > b[1];
  const overlapArea = (a, b) =>
    Math.max(0, Math.min(a[2], b[2]) - Math.max(a[0], b[0])) * Math.max(0, Math.min(a[3], b[3]) - Math.max(a[1], b[1]));

  // Finds the cheapest free side for a w x h label; fills `out`, returns it or null.
  function searchSides(view, views, w, h, out) {
    const rect = scratchRect;
    const [gx, gy] = view.scrGround;
    const [tx, ty] = view.scrTop;
    const midY = (gy + ty) / 2;
    const sides = LABEL_SIDES[view.data.kind] ?? LABEL_SIDES.default;
    out.side = null;
    out.cost = Infinity;
    for (let si = 0; si < sides.length; si++) {
      const side = sides[si];
      let px;
      let py;
      if (side === 'top') {
        px = tx;
        py = ty - LABEL_GAP_PX;
        rect[0] = px - w / 2;
        rect[1] = py - h;
      } else if (side === 'topRight') {
        px = tx + view.screenR * 0.5;
        py = ty - LABEL_GAP_PX;
        rect[0] = px;
        rect[1] = py - h;
      } else if (side === 'topLeft') {
        px = tx - view.screenR * 0.5;
        py = ty - LABEL_GAP_PX;
        rect[0] = px - w;
        rect[1] = py - h;
      } else if (side === 'bottom') {
        px = gx;
        py = gy + view.screenDepth + LABEL_GAP_PX;
        rect[0] = px - w / 2;
        rect[1] = py;
      } else if (side === 'right') {
        px = gx + view.screenR + LABEL_GAP_PX;
        py = midY;
        rect[0] = px;
        rect[1] = py - h / 2;
      } else {
        px = gx - view.screenR - LABEL_GAP_PX;
        py = midY;
        rect[0] = px - w;
        rect[1] = py - h / 2;
      }
      rect[2] = rect[0] + w;
      rect[3] = rect[1] + h;
      // A clipped label is unreadable; hide it rather than cut it off.
      if (rect[0] < -2 || rect[1] < -2 || rect[2] > size.width + 2 || rect[3] > size.height + 2) continue;
      let blocked = false;
      for (const other of placed) {
        if (overlaps(rect, other)) {
          blocked = true;
          break;
        }
      }
      if (blocked) continue;
      // Penalise by the share of each other node the label would hide.
      let cover = 0;
      for (const other of views) {
        if (other === view) continue;
        const r = other.nodeRect;
        const weight = COVER_WEIGHT[other.data.kind] ?? 1;
        cover += (weight * overlapArea(rect, r)) / Math.max(1, (r[2] - r[0]) * (r[3] - r[1]));
      }
      const cost = si * 0.15 + 2.5 * cover - (view.labelSide === side ? 0.1 : 0);
      if (cost < out.cost) {
        out.cost = cost;
        out.side = side;
        out.ax = px;
        out.ay = py;
        for (let i = 0; i < 4; i++) out.rect[i] = rect[i];
      }
    }
    return out.side ? out : null;
  }

  function copyChoice(from, to) {
    to.side = from.side;
    to.cost = from.cost;
    to.ax = from.ax;
    to.ay = from.ay;
    for (let i = 0; i < 4; i++) to.rect[i] = from.rect[i];
  }

  function setCompact(view, compact) {
    if (view.compact === compact) return;
    view.compact = compact;
    view.button.classList.toggle('is-compact', compact);
    // Out of flow but still laid out, so its size stays measurable.
    view.sub.style.position = compact ? 'absolute' : '';
    view.sub.style.visibility = compact ? 'hidden' : '';
  }

  function layoutLabels() {
    labelsDirty = false;
    // Deferred to the frame so a pointer moving from a node's mesh onto its
    // label (leave, then enter) keeps the reveal.
    if (!hoverNodeId && !focusId) revealId = null;
    const { width, height } = size;
    if (!width || !height) return;
    camera.updateMatrixWorld();
    const ppu = (height / (camera.top - camera.bottom)) * camera.zoom;
    camRight.setFromMatrixColumn(camera.matrixWorld, 0).applyQuaternion(layoutInverse);
    camUp.setFromMatrixColumn(camera.matrixWorld, 1).applyQuaternion(layoutInverse);
    const views = [];
    for (const view of nodeViews.values()) {
      if (view.leaving) setLabelHidden(view, true);
      else views.push(view);
    }
    // Batch DOM reads before any writes. Full and compact sizes are derived
    // from whichever state the label is in now.
    for (const view of views) {
      const bw = view.button.offsetWidth;
      const bh = view.button.offsetHeight;
      if (!bw || !bh) continue;
      const hasSub = Boolean(view.data.sublabel);
      const subW = hasSub ? view.sub.offsetWidth : 0;
      const subH = hasSub ? view.sub.offsetHeight : 0;
      const extraW = Math.max(0, subW - view.title.offsetWidth);
      if (view.compact) {
        view.compactW = bw;
        view.compactH = bh;
        view.fullW = bw + extraW;
        view.fullH = bh + subH;
      } else {
        view.fullW = bw;
        view.fullH = bh;
        view.compactW = bw - extraW;
        view.compactH = bh - subH;
      }
    }
    for (const view of views) {
      const g = view.group.position;
      const s = view.group.scale.x;
      const ground = toScreen(g.x, 0, g.z, view.scrGround ?? (view.scrGround = [0, 0]));
      const top = toScreen(g.x, view.spec.height * s, g.z, view.scrTop ?? (view.scrTop = [0, 0]));
      const r = view.spec.radius * s * ppu;
      const depth = r * 0.62;
      view.nodeRect = view.nodeRect ?? [0, 0, 0, 0];
      view.nodeRect[0] = Math.min(ground[0], top[0]) - r;
      view.nodeRect[1] = Math.min(ground[1], top[1]) - depth;
      view.nodeRect[2] = Math.max(ground[0], top[0]) + r;
      view.nodeRect[3] = Math.max(ground[1], top[1]) + depth;
      view.screenR = r;
      view.screenDepth = depth;
    }
    views.sort((a, b) => labelPriority(a) - labelPriority(b) || (a.id < b.id ? -1 : 1));
    placed.length = 0;
    // Zone names are small and structural: place them first as obstacles.
    for (const view of zoneViews.values()) {
      if (view.leaving) continue;
      const w = view.el.offsetWidth || 60;
      const h = view.el.offsetHeight || 12;
      const [x, y] = toScreen(view.label.position.x, view.label.position.y, view.label.position.z, scratchRect);
      const box = [x, y - h, x + w, y];
      const blocked = placed.some((other) => overlaps(box, other));
      view.el.style.visibility = blocked ? 'hidden' : '';
      if (!blocked) placed.push(box);
    }
    for (const view of views) {
      const pinned = view.id === selectedId || revealsOnHover(view);
      let choice = searchSides(view, views, view.fullW ?? 80, view.fullH ?? 30, candidate);
      let compact = false;
      if (choice) copyChoice(choice, (choice = fullChoice));
      if (!pinned && view.data.sublabel && (!choice || choice.cost > LABEL_COMPACT_COST)) {
        const alt = searchSides(view, views, view.compactW ?? 60, view.compactH ?? 18, candidate);
        if (alt && (!choice || alt.cost < choice.cost - 0.3)) {
          choice = alt;
          compact = true;
        }
      }
      setCompact(view, compact);
      if (!choice) {
        setLabelHidden(view, true);
        view.labelSide = null;
        continue;
      }
      placed.push(choice.rect.slice());
      view.labelSide = choice.side;
      setLabelHidden(view, false);
      const [cx, cy] = LABEL_CENTER[choice.side];
      view.label.center.set(cx, cy);
      // Anchor the label in 3D so CSS2DRenderer projects it onto (ax, ay):
      // offset from the node top along the camera's right/up axes.
      const s = view.group.scale.x || 1;
      const [tx, ty] = view.scrTop;
      view.label.position
        .set(0, view.spec.height, 0)
        .addScaledVector(camRight, (choice.ax - tx) / ppu / s)
        .addScaledVector(camUp, -(choice.ay - ty) / ppu / s);
    }
  }

  // --- Selection & hover ---------------------------------------------------
  function attachSelectRing(view) {
    view.group.add(selectRing);
    selectRing.position.set(0, 0.012, 0);
    selectRing.scale.setScalar(view.spec.radius + 0.18);
    selectRing.visible = true;
  }

  function detachSelectRing() {
    selectRing.parent?.remove(selectRing);
    selectRing.visible = false;
  }

  function setSelected(id) {
    const view = id ? nodeViews.get(id) : null;
    const next = view && !view.leaving ? id : null;
    if (selectedId && nodeViews.has(selectedId)) nodeViews.get(selectedId).button.classList.remove('is-selected');
    selectedId = next;
    detachSelectRing();
    if (view && next) {
      view.button.classList.add('is-selected');
      attachSelectRing(view);
    }
    labelsDirty = true;
    invalidate();
  }

  function setHoverNode(id) {
    if (hoverNodeId === id) return;
    if (id && id !== revealId) revealId = nodeViews.get(id)?.labelSide === null ? id : null;
    const prev = hoverNodeId ? nodeViews.get(hoverNodeId) : null;
    hoverNodeId = id;
    if (prev) applyNodeLook(prev);
    const next = id ? nodeViews.get(id) : null;
    if (next) applyNodeLook(next);
    canvas.style.cursor = id ? 'pointer' : '';
    labelsDirty = true;
    invalidate();
  }

  function setHoverEdge(id, x = 0, y = 0) {
    if (hoverEdgeId !== id) {
      const prev = hoverEdgeId ? edgeViews.get(hoverEdgeId) : null;
      hoverEdgeId = id;
      if (prev) applyEdgeLook(prev);
      const next = id ? edgeViews.get(id) : null;
      if (next) applyEdgeLook(next);
      invalidate();
    }
    const view = id ? edgeViews.get(id) : null;
    if (view && view.data.label) {
      tooltip.textContent = view.data.label;
      tooltip.style.display = '';
      // Measure once shown, then keep it inside the container: flip to the
      // other side of the cursor near an edge, and clamp as a last resort.
      const w = tooltip.offsetWidth;
      const h = tooltip.offsetHeight;
      let left = x + TOOLTIP_OFFSET_PX;
      let top = y + TOOLTIP_OFFSET_PX;
      if (left + w > size.width) left = x - TOOLTIP_OFFSET_PX - w;
      if (top + h > size.height) top = y - TOOLTIP_OFFSET_PX - h;
      tooltip.style.left = `${clamp(left, 0, Math.max(0, size.width - w))}px`;
      tooltip.style.top = `${clamp(top, 0, Math.max(0, size.height - h))}px`;
    } else {
      tooltip.style.display = 'none';
    }
  }

  function pointerToNdc(event) {
    const rect = canvas.getBoundingClientRect();
    ndc.set(((event.clientX - rect.left) / rect.width) * 2 - 1, -((event.clientY - rect.top) / rect.height) * 2 + 1);
    return rect;
  }

  function pickNode(event) {
    pointerToNdc(event);
    raycaster.setFromCamera(ndc, camera);
    const hits = raycaster.intersectObjects(pickables, false);
    for (const hit of hits) {
      const id = hit.object.userData.nodeId;
      if (id && nodeViews.has(id) && !nodeViews.get(id).leaving) return id;
    }
    return null;
  }

  function pickEdge(px, py, rect) {
    let best = null;
    let bestD = EDGE_HOVER_PX;
    const w = rect.width;
    const h = rect.height;
    for (const view of edgeViews.values()) {
      if (view.leaving || view.appear < 0.5) continue;
      let ax = 0;
      let ay = 0;
      for (let i = 0; i <= EDGE_SAMPLES; i++) {
        tmpPos.fromArray(view.samples, i * 3).project(camera);
        const bx = ((tmpPos.x + 1) / 2) * w;
        const by = ((1 - tmpPos.y) / 2) * h;
        if (i > 0) {
          const d = segmentDistance(px, py, ax, ay, bx, by);
          if (d < bestD) {
            bestD = d;
            best = view.id;
          }
        }
        ax = bx;
        ay = by;
      }
    }
    return best;
  }

  let downX = 0;
  let downY = 0;
  let downId = null;
  // Registered before controls.connect(), so this runs ahead of OrbitControls'
  // own pointerdown and its 'start' event.
  function onPointerDown(event) {
    downId = event.pointerId;
    downX = event.clientX;
    downY = event.clientY;
    movedBeforeGesture = userMoved;
    setHoverEdge(null);
  }
  function onPointerUp(event) {
    if (event.pointerId !== downId) return;
    downId = null;
    if (Math.hypot(event.clientX - downX, event.clientY - downY) >= CLICK_SLOP_PX) return;
    // A press without movement is a click, not a camera gesture, so it must
    // not freeze the responsive pose and fit.
    userMoved = movedBeforeGesture;
    // Right and middle buttons drive the camera; only the primary button selects.
    if (event.button !== 0) return;
    onSelect?.(pickNode(event));
  }
  // Sent when the browser takes over a touch (e.g. to scroll the page).
  function onPointerCancel(event) {
    if (event.pointerId === downId) downId = null;
  }
  function onPointerMove(event) {
    if (downId !== null || event.buttons) return;
    const id = pickNode(event);
    setHoverNode(id);
    if (id) {
      setHoverEdge(null);
      return;
    }
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    setHoverEdge(pickEdge(x, y, rect), x, y);
  }
  function onPointerLeave() {
    setHoverNode(null);
    setHoverEdge(null);
  }
  canvas.addEventListener('pointerdown', onPointerDown);
  canvas.addEventListener('pointerup', onPointerUp);
  canvas.addEventListener('pointermove', onPointerMove);
  canvas.addEventListener('pointerleave', onPointerLeave);
  canvas.addEventListener('pointercancel', onPointerCancel);
  controls.connect(canvas);
  // connect() sets touch-action: none; let the browser keep vertical scrolling.
  if (coarsePointer) canvas.style.touchAction = 'pan-y';

  // --- Camera fit ----------------------------------------------------------
  function cornersOf(b) {
    const out = [];
    for (const u of [b.u0, b.u1]) for (const v of [b.v0, b.v1]) for (const y of [b.y0, b.y1]) out.push(new THREE.Vector3(u, y, v));
    return out;
  }
  const fullCorners = cornersOf(BOUNDS);
  let fitCorners = fullCorners;
  let fitKey = '';
  let camTween = null;
  const tweenTarget = new THREE.Vector3();

  // Frame the union of the zones on screen, easing there unless this is the
  // first build. A camera the user has moved is left alone.
  function refitToZones(zones, animate) {
    if (!zones.length) return;
    const b = { u0: Infinity, u1: -Infinity, v0: Infinity, v1: -Infinity, y0: BOUNDS.y0, y1: BOUNDS.y1 };
    for (const { rect } of zones) {
      b.u0 = Math.min(b.u0, rect[0]);
      b.v0 = Math.min(b.v0, rect[1]);
      b.u1 = Math.max(b.u1, rect[2]);
      b.v1 = Math.max(b.v1, rect[3]);
    }
    const key = [b.u0, b.v0, b.u1, b.v1].join('|');
    if (key === fitKey) return;
    fitKey = key;
    fitCorners = cornersOf(b);
    if (!userMoved) applyFit(false, animate);
  }

  function fitFor(dir, aspect, corners = fitCorners) {
    const forward = new THREE.Vector3().copy(dir).negate();
    const right = new THREE.Vector3().crossVectors(forward, UP).normalize();
    const up = new THREE.Vector3().crossVectors(right, forward).normalize();
    let x0 = Infinity;
    let x1 = -Infinity;
    let y0 = Infinity;
    let y1 = -Infinity;
    for (const c of corners) {
      const p = layout.localToWorld(tmpPos.copy(c));
      const x = p.dot(right);
      const y = p.dot(up);
      x0 = Math.min(x0, x);
      x1 = Math.max(x1, x);
      y0 = Math.min(y0, y);
      y1 = Math.max(y1, y);
    }
    let halfW = ((x1 - x0) / 2) * 1.02;
    let halfH = ((y1 - y0) / 2) * 1.04;
    if (halfW / halfH > aspect) halfH = halfW / aspect;
    else halfW = halfH * aspect;
    // Pivot on the ground plane so orbiting feels anchored to the diorama.
    const target = new THREE.Vector3().addScaledVector(right, (x0 + x1) / 2).addScaledVector(up, (y0 + y1) / 2);
    target.addScaledVector(forward, -target.y / forward.y);
    return { halfH, target };
  }

  function choosePose(aspect) {
    const land = fitFor(POSES.landscape, aspect, fullCorners);
    const port = fitFor(POSES.portrait, aspect, fullCorners);
    return port.halfH * 1.25 < land.halfH ? 'portrait' : 'landscape';
  }

  function applyFit(reset, animate = false) {
    const { width, height } = size;
    if (!width || !height) return;
    const aspect = width / height;
    const place = reset || !userMoved;
    if (place) {
      const next = choosePose(aspect);
      if (next !== pose) {
        pose = next;
        onPoseChange();
      }
    }
    const fit = fitFor(POSES[pose], aspect);
    if (reset) {
      // Drain orbit/zoom momentum first or damping keeps moving the camera
      // away from the pose we are about to restore.
      const damping = controls.enableDamping;
      controls.enableDamping = false;
      controls.update();
      controls.enableDamping = damping;
    }
    if (animate && place && !reset && !reducedMotion) {
      camTween = { t: 0, fromH: baseHalfH, toH: fit.halfH, from: controls.target.clone(), to: fit.target };
      invalidate();
      return;
    }
    camTween = null;
    setFrame(fit.halfH, fit.target, aspect, place);
  }

  function setFrame(halfH, target, aspect, place) {
    baseHalfH = halfH;
    camera.top = halfH;
    camera.bottom = -halfH;
    camera.left = -halfH * aspect;
    camera.right = halfH * aspect;
    if (place) {
      controls.target.copy(target);
      camera.position.copy(target).addScaledVector(POSES[pose], CAMERA_DIST);
      camera.zoom = 1;
      camera.lookAt(target);
    }
    camera.updateProjectionMatrix();
    controls.update();
    updateDensity();
  }

  // Advanced from stepTweens so it keeps rendering while paused.
  function stepCameraTween(dt) {
    // Held while a press is in progress; a real drag keeps userMoved set, so
    // the tween never resumes over the user's camera. A plain click resumes it.
    if (!camTween || userMoved || !size.width || !size.height) return false;
    camTween.t = Math.min(1, camTween.t + dt / CAMERA_TWEEN_S);
    const k = easeOut(camTween.t);
    tweenTarget.lerpVectors(camTween.from, camTween.to, k);
    setFrame(THREE.MathUtils.lerp(camTween.fromH, camTween.toH, k), tweenTarget, size.width / size.height, true);
    labelsDirty = true;
    if (camTween.t >= 1) camTween = null;
    return true;
  }

  // Bodies, zone tabs and edge bows are all drawn relative to the camera's
  // front, which turns a quarter between poses.
  function poseYaw() {
    return pose === 'portrait' ? Math.PI / 2 : 0;
  }

  function onPoseChange() {
    for (const view of zoneViews.values()) placeZoneLabel(view);
    for (const view of nodeViews.values()) view.body.rotation.y = poseYaw();
    for (const view of edgeViews.values()) {
      const from = nodeViews.get(view.data.from);
      const to = nodeViews.get(view.data.to);
      if (!from || !to) continue;
      buildEdgeGeometry(view, from, to);
      applyEdgeLook(view);
    }
    labelsDirty = true;
  }

  function updateDensity() {
    if (!size.height) return;
    const pxPerUnit = (size.height / (2 * baseHalfH)) * camera.zoom;
    overlay.dataset.density = pxPerUnit < 30 ? 'compact' : 'normal';
  }

  function resize() {
    const width = container.clientWidth;
    const height = container.clientHeight;
    if (width === size.width && height === size.height) return;
    size = { width, height };
    if (!width || !height) return;
    renderer.setSize(width, height, false);
    labelRenderer.setSize(width, height);
    applyFit(false);
    labelsDirty = true;
    invalidate();
  }

  // --- Lights --------------------------------------------------------------
  hemi.color.set(theme.hemiSky);
  hemi.groundColor.set(theme.hemiGround);
  hemi.intensity = theme.hemi;
  sun.intensity = theme.sun;

  // --- Loop ----------------------------------------------------------------
  const animating = () => !paused && !hidden && inView && !reducedMotion && !disposed;

  function invalidate() {
    if (disposed || loopOn) return;
    loopOn = true;
    lastNow = performance.now();
    renderer.setAnimationLoop(frame);
  }

  function frame(now) {
    const dt = clamp((now - lastNow) / 1000, 0, MAX_DT);
    lastNow = now;
    const anim = animating();
    if (anim) {
      simTime += dt;
      stepSim(dt);
      if (selectRing.visible) selectRingMat.opacity = 0.6 + 0.3 * Math.sin(simTime * 4);
    }
    const tweening = stepTweens(reducedMotion ? FADE_S : dt);
    const moving = controls.update();
    if (size.width && size.height) {
      if (labelsDirty || tweening || moving) layoutLabels();
      renderer.render(scene, camera);
      labelRenderer.render(scene, camera);
    }
    if (!anim && !tweening && !moving) {
      loopOn = false;
      renderer.setAnimationLoop(null);
    }
  }

  // Fires on pointerdown too; onPointerUp undoes this when the press was a click.
  controls.addEventListener('start', () => {
    userMoved = true;
    setHoverEdge(null);
  });
  controls.addEventListener('change', () => {
    updateDensity();
    labelsDirty = true;
    invalidate();
  });

  const onVisibility = () => {
    hidden = document.hidden;
    if (!hidden) invalidate();
  };
  document.addEventListener('visibilitychange', onVisibility);
  // three rebuilds its GL state on restore but nothing redraws a scene that is
  // paused, hidden or in reduced motion.
  const onContextRestored = () => {
    labelsDirty = true;
    invalidate();
  };
  canvas.addEventListener('webglcontextrestored', onContextRestored);

  const resizeObserver = new ResizeObserver(resize);
  resizeObserver.observe(container);
  const intersectionObserver =
    typeof IntersectionObserver === 'function'
      ? new IntersectionObserver((entries) => {
          inView = entries[entries.length - 1].isIntersecting;
          if (inView) invalidate();
        })
      : null;
  intersectionObserver?.observe(container);
  resize();

  // --- Public API ----------------------------------------------------------
  const controller = {
    update,
    select(id) {
      setSelected(id ?? null);
    },
    setPaused(value) {
      paused = Boolean(value);
      invalidate();
    },
    resetView() {
      userMoved = false;
      applyFit(true);
      invalidate();
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      renderer.setAnimationLoop(null);
      resizeObserver.disconnect();
      intersectionObserver?.disconnect();
      document.removeEventListener('visibilitychange', onVisibility);
      canvas.removeEventListener('pointerdown', onPointerDown);
      canvas.removeEventListener('pointerup', onPointerUp);
      canvas.removeEventListener('pointermove', onPointerMove);
      canvas.removeEventListener('pointerleave', onPointerLeave);
      canvas.removeEventListener('pointercancel', onPointerCancel);
      canvas.removeEventListener('webglcontextrestored', onContextRestored);
      controls.dispose();
      for (const view of [...edgeViews.values()]) destroyEdgeView(view);
      for (const view of [...nodeViews.values()]) destroyNodeView(view);
      for (const view of [...zoneViews.values()]) destroyZoneView(view);
      for (const g of geoCache.values()) g.dispose();
      geoCache.clear();
      for (const m of [cubeMat, sphereMat, hopperMat, selectRingMat]) m.dispose();
      for (const mesh of [cubeMesh, sphereMesh, hopperMesh]) mesh.dispose();
      shadowTex.dispose();
      dashTex.dispose();
      renderer.dispose();
      renderer.forceContextLoss();
      canvas.remove();
      overlay.remove();
      tooltip.remove();
      defaults.remove();
      if (debug && window.__sbScene === controller) delete window.__sbScene;
    },
    info() {
      return {
        geometries: renderer.info.memory.geometries,
        textures: renderer.info.memory.textures,
        nodes: nodeViews.size,
        edges: edgeViews.size,
        particles: cubeCount + sphereCount,
      };
    },
  };
  if (debug) window.__sbScene = controller;
  return controller;
}

function segmentDistance(px, py, ax, ay, bx, by) {
  const dx = bx - ax;
  const dy = by - ay;
  const len2 = dx * dx + dy * dy;
  const t = len2 > 0 ? clamp(((px - ax) * dx + (py - ay) * dy) / len2, 0, 1) : 0;
  return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
}

// Soft radial blob used as a fake contact shadow under every node.
function makeShadowTexture() {
  const c = document.createElement('canvas');
  c.width = c.height = 64;
  const ctx = c.getContext('2d');
  const g = ctx.createRadialGradient(32, 32, 0, 32, 32, 32);
  g.addColorStop(0, 'rgba(255,255,255,1)');
  g.addColorStop(0.55, 'rgba(255,255,255,0.45)');
  g.addColorStop(1, 'rgba(255,255,255,0)');
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, 64, 64);
  const tex = new THREE.CanvasTexture(c);
  tex.colorSpace = THREE.NoColorSpace;
  return tex;
}

// 1-D on/off pattern for dashed control edges (alpha map, repeats along U).
function makeDashTexture() {
  const c = document.createElement('canvas');
  c.width = 16;
  c.height = 1;
  const ctx = c.getContext('2d');
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, 16, 1);
  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, 9, 1);
  const tex = new THREE.CanvasTexture(c);
  tex.wrapS = THREE.RepeatWrapping;
  tex.colorSpace = THREE.NoColorSpace;
  tex.magFilter = THREE.NearestFilter;
  tex.minFilter = THREE.LinearFilter;
  tex.generateMipmaps = false;
  return tex;
}
