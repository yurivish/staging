// Datastar Pro v1.0.1 licensed to GitHub user @yurivish
var un = /🖕JS_DS🚀/.source,
  Et = un.slice(0, 5),
  xt = un.slice(4),
  ne = "datastar-fetch",
  Me = "datastar-prop-change",
  Ze = "datastar-ready",
  ve = "datastar-scope-children",
  fe = "datastar-signal-patch";
var W = Object.hasOwn ?? Object.prototype.hasOwnProperty.call;
var ae = (e) =>
    e !== null &&
    typeof e == "object" &&
    (Object.getPrototypeOf(e) === Object.prototype ||
      Object.getPrototypeOf(e) === null),
  Qe = (e) => {
    for (let t in e) if (W(e, t)) return !1;
    return !0;
  },
  de = (e, t) => {
    for (let r in e) {
      let o = e[r];
      ae(o) || Array.isArray(o) ? de(o, t) : (e[r] = t(o));
    }
  },
  Ye = (e) => {
    let t = {};
    for (let [r, o] of e) {
      let n = r.split("."),
        i = n.pop(),
        s = n.reduce((a, c) => (a[c] ??= {}), t);
      s[i] = o;
    }
    return t;
  };
var Xe = [],
  kt = [],
  st = 0,
  et = 0,
  Nt = 0,
  Pt,
  re,
  tt = 0,
  F = () => {
    st++;
  },
  H = () => {
    --st || (pn(), ce());
  },
  V = (e) => {
    (Pt = re), (re = e);
  },
  q = () => {
    (re = Pt), (Pt = void 0);
  },
  Oe = (e) => Nr.bind(0, { previousValue: e, t: e, e: 1 }),
  Mt = Symbol("computed"),
  pe = (e) => {
    let t = Pr.bind(0, { e: 17, getter: e });
    return (t[Mt] = 1), t;
  },
  M = (e) => {
    let t = { d: e, e: 2 };
    re && Dt(t, re), V(t), F();
    try {
      t.d();
    } finally {
      H(), q();
    }
    return yn.bind(0, t);
  },
  pn = () => {
    for (; et < Nt; ) {
      let e = kt[et];
      (kt[et++] = void 0), hn(e, (e.e &= -65));
    }
    (et = 0), (Nt = 0);
  },
  fn = (e) => ("getter" in e ? mn(e) : gn(e, e.t)),
  mn = (e) => {
    V(e), bn(e);
    try {
      let t = e.t;
      return t !== (e.t = e.getter(t));
    } finally {
      q(), Tn(e);
    }
  },
  gn = (e, t) => ((e.e = 1), e.previousValue !== (e.previousValue = t)),
  Ot = (e) => {
    let t = e.e;
    if (!(t & 64)) {
      e.e = t | 64;
      let r = e.r;
      r ? Ot(r.i) : (kt[Nt++] = e);
    }
  },
  hn = (e, t) => {
    if (t & 16 || (t & 32 && vn(e.o, e))) {
      V(e), bn(e), F();
      try {
        e.d();
      } finally {
        H(), q(), Tn(e);
      }
      return;
    }
    t & 32 && (e.e = t & -33);
    let r = e.o;
    for (; r; ) {
      let o = r.c,
        n = o.e;
      n & 64 && hn(o, (o.e = n & -65)), (r = r.s);
    }
  },
  Nr = (e, ...t) => {
    if (t.length) {
      if (e.t !== (e.t = t[0])) {
        e.e = 17;
        let o = e.r;
        return o && (Mr(o), st || pn()), !0;
      }
      return !1;
    }
    let r = e.t;
    if (e.e & 16 && gn(e, r)) {
      let o = e.r;
      o && ot(o);
    }
    return re && Dt(e, re), r;
  },
  Pr = (e) => {
    let t = e.e;
    if (t & 16 || (t & 32 && vn(e.o, e))) {
      if (mn(e)) {
        let r = e.r;
        r && ot(r);
      }
    } else t & 32 && (e.e = t & -33);
    return re && Dt(e, re), e.t;
  },
  yn = (e) => {
    let t = e.o;
    for (; t; ) t = rt(t, e);
    let r = e.r;
    r && rt(r), (e.e = 0);
  },
  Dt = (e, t) => {
    let r = t.a;
    if (r && r.c === e) return;
    let o = r ? r.s : t.o;
    if (o && o.c === e) {
      (o.p = tt), (t.a = o);
      return;
    }
    let n = e.m;
    if (n && n.p === tt && n.i === t) return;
    let i = (t.a = e.m = { p: tt, c: e, i: t, l: r, s: o, u: n });
    o && (o.l = i), r ? (r.s = i) : (t.o = i), n ? (n.n = i) : (e.r = i);
  },
  rt = (e, t = e.i) => {
    let r = e.c,
      o = e.l,
      n = e.s,
      i = e.n,
      s = e.u;
    if (
      (n ? (n.l = o) : (t.a = o),
      o ? (o.s = n) : (t.o = n),
      i ? (i.u = s) : (r.m = s),
      s)
    )
      s.n = i;
    else if (!(r.r = i))
      if ("getter" in r) {
        let a = r.o;
        if (a) {
          r.e = 17;
          do a = rt(a, r);
          while (a);
        }
      } else "previousValue" in r || yn(r);
    return n;
  },
  Mr = (e) => {
    let t = e.n,
      r;
    e: for (;;) {
      let o = e.i,
        n = o.e;
      if (
        (n & 60
          ? n & 12
            ? n & 4
              ? !(n & 48) && Or(e, o)
                ? ((o.e = n | 40), (n &= 1))
                : (n = 0)
              : (o.e = (n & -9) | 32)
            : (n = 0)
          : (o.e = n | 32),
        n & 2 && Ot(o),
        n & 1)
      ) {
        let i = o.r;
        if (i) {
          let s = (e = i).n;
          s && ((r = { t, f: r }), (t = s));
          continue;
        }
      }
      if ((e = t)) {
        t = e.n;
        continue;
      }
      for (; r; )
        if (((e = r.t), (r = r.f), e)) {
          t = e.n;
          continue e;
        }
      break;
    }
  },
  bn = (e) => {
    tt++, (e.a = void 0), (e.e = (e.e & -57) | 4);
  },
  Tn = (e) => {
    let t = e.a,
      r = t ? t.s : e.o;
    for (; r; ) r = rt(r, e);
    e.e &= -5;
  },
  vn = (e, t) => {
    let r,
      o = 0,
      n = !1;
    e: for (;;) {
      let i = e.c,
        s = i.e;
      if (t.e & 16) n = !0;
      else if ((s & 17) === 17) {
        if (fn(i)) {
          let a = i.r;
          a.n && ot(a), (n = !0);
        }
      } else if ((s & 33) === 33) {
        (e.n || e.u) && (r = { t: e, f: r }), (e = i.o), (t = i), ++o;
        continue;
      }
      if (!n) {
        let a = e.s;
        if (a) {
          e = a;
          continue;
        }
      }
      for (; o--; ) {
        let a = t.r,
          c = a.n;
        if ((c ? ((e = r.t), (r = r.f)) : (e = a), n)) {
          if (fn(t)) {
            c && ot(a), (t = e.i);
            continue;
          }
          n = !1;
        } else t.e &= -33;
        if (((t = e.i), e.s)) {
          e = e.s;
          continue e;
        }
      }
      return n;
    }
  },
  ot = (e) => {
    do {
      let t = e.i,
        r = t.e;
      (r & 48) === 32 && ((t.e = r | 16), r & 2 && Ot(t));
    } while ((e = e.n));
  },
  Or = (e, t) => {
    let r = t.a;
    for (; r; ) {
      if (r === e) return !0;
      r = r.l;
    }
    return !1;
  },
  X = (e) => {
    let t = Z,
      r = e.split(".");
    for (let o of r) {
      if (t == null || !W(t, o)) return;
      t = t[o];
    }
    return t;
  },
  nt = (e, t = "") => {
    let r = Array.isArray(e);
    if (r || ae(e)) {
      let o = r ? [] : {};
      for (let i in e) o[i] = Oe(nt(e[i], `${t + i}.`));
      let n = Oe(0);
      return new Proxy(o, {
        get(i, s) {
          if (!(s === "toJSON" && !W(o, s)))
            return r && s in Array.prototype
              ? (n(), o[s])
              : typeof s == "symbol"
                ? o[s]
                : ((!W(o, s) || o[s]() == null) &&
                    ((o[s] = Oe("")), ce(t + s, ""), n(n() + 1)),
                  o[s]());
        },
        set(i, s, a) {
          let c = t + s;
          if (r && s === "length") {
            let l = o[s] - a;
            if (((o[s] = a), l > 0)) {
              let u = {};
              for (let d = a; d < o[s]; d++) u[d] = null;
              ce(t.slice(0, -1), u), n(n() + 1);
            }
          } else if (W(o, s))
            if (a == null) delete o[s];
            else if (W(a, Mt)) (o[s] = a), ce(c, "");
            else {
              let l = o[s](),
                u = `${c}.`;
              if (ae(l) && ae(a)) {
                for (let d in l) W(a, d) || (delete l[d], ce(u + d, null));
                for (let d in a) {
                  let p = a[d];
                  l[d] !== p && (l[d] = p);
                }
              } else o[s](nt(a, u)) && ce(c, a);
            }
          else
            a != null &&
              (W(a, Mt)
                ? ((o[s] = a), ce(c, ""))
                : ((o[s] = Oe(nt(a, `${c}.`))), ce(c, a)),
              n(n() + 1));
          return !0;
        },
        deleteProperty(i, s) {
          return delete o[s], n(n() + 1), !0;
        },
        ownKeys() {
          return n(), Reflect.ownKeys(o);
        },
        has(i, s) {
          return n(), s in o;
        },
      });
    }
    return e;
  },
  ce = (e, t) => {
    if ((e !== void 0 && t !== void 0 && Xe.push([e, t]), !st && Xe.length)) {
      let r = Ye(Xe);
      (Xe.length = 0),
        document.dispatchEvent(new CustomEvent(fe, { detail: r }));
    }
  },
  U = (e, { ifMissing: t } = {}) => {
    F();
    for (let r in e) e[r] == null ? t || delete Z[r] : Sn(e[r], r, Z, "", t);
    H();
  },
  N = (e, t) => U(Ye(e), t),
  Sn = (e, t, r, o, n) => {
    if (ae(e)) {
      (W(r, t) && (ae(r[t]) || Array.isArray(r[t]))) || (r[t] = {});
      for (let i in e)
        e[i] == null ? n || delete r[t][i] : Sn(e[i], i, r[t], `${o + t}.`, n);
    } else (n && W(r, t)) || (r[t] = e);
  },
  dn = (e) => (typeof e == "string" ? RegExp(e.replace(/^\/|\/$/g, "")) : e),
  B = ({ include: e = /.*/, exclude: t = /(?!)/ } = {}, r = Z) => {
    let o = dn(e),
      n = dn(t),
      i = [],
      s = [[r, ""]];
    for (; s.length; ) {
      let [a, c] = s.pop();
      for (let l in a) {
        let u = c + l;
        ae(a[l])
          ? s.push([a[l], `${u}.`])
          : o.test(u) && !n.test(u) && i.push([u, X(u)]);
      }
    }
    return Ye(i);
  },
  Z = nt({});
var oe = (e) =>
  e instanceof HTMLElement ||
  e instanceof SVGElement ||
  e instanceof MathMLElement;
var ee = (e) =>
    e
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
      .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
      .replace(/([a-z])([0-9]+)/gi, "$1-$2")
      .replace(/([0-9]+)([a-z])/gi, "$1-$2")
      .replace(/[\s_]+/g, "-")
      .toLowerCase(),
  Rn = (e) => ee(e).replace(/-./g, (t) => t[1].toUpperCase()),
  De = (e) => ee(e).replace(/-/g, "_");
var Dr =
    /^(?:(?:async\s+)?function\b|(?:async\s*)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>)/,
  le = (e, t = {}) => {
    let { reviveFunctionStrings: r = !1 } = t;
    try {
      return r
        ? JSON.parse(e, (o, n) => {
            if (typeof n != "string") return n;
            let i = n.trim();
            if (!Dr.test(i)) return n;
            try {
              let s = Function(`return (${i})`)();
              return typeof s == "function" ? s : n;
            } catch {
              return n;
            }
          })
        : JSON.parse(e);
    } catch {
      return Function(`return (${e})`)();
    }
  },
  Cn = {
    camel: (e) => e.replace(/-[a-z]/g, (t) => t[1].toUpperCase()),
    snake: (e) => e.replace(/-/g, "_"),
    pascal: (e) => e[0].toUpperCase() + Cn.camel(e.slice(1)),
  },
  _ = (e, t, r = "camel") => {
    for (let o of t.get("case") || [r]) e = Cn[o]?.(e) || e;
    return e;
  },
  se = (e) => `data-${e}`,
  Lt = (e) => e;
var Lr = "https://data-star.dev/errors",
  Le = (e, t, r = {}) => {
    Object.assign(r, e);
    let o = new Error(),
      n = De(t),
      i = new URLSearchParams({ metadata: JSON.stringify(r) }).toString(),
      s = JSON.stringify(r, null, 2);
    return (
      (o.message = `${t}
More info: ${Lr}/${n}?${i}
Context: ${s}`),
      o
    );
  },
  $e = new Map(),
  $t = new Map(),
  En = new Map(),
  Fe = new Proxy(
    {},
    {
      get: (e, t) => $e.get(t)?.apply,
      has: (e, t) => $e.has(t),
      ownKeys: () => Reflect.ownKeys($e),
      set: () => !1,
      deleteProperty: () => !1,
    },
  ),
  Ie = new Map(),
  it = [],
  It = new Set(),
  Se = new Set(),
  An = !1,
  A = (e) => {
    it.push(e),
      it.length === 1 &&
        setTimeout(() => {
          for (let r of it) It.add(r.name), $t.set(r.name, r);
          it.length = 0;
          let t = Se.size ? [...Se] : [document.documentElement];
          for (let r of t) Hr(r, !Se.has(r));
          It.clear();
        });
  },
  L = (e) => {
    $e.set(e.name, e);
  };
document.addEventListener(ne, (e) => {
  let t = En.get(e.detail.type);
  t &&
    t.apply(
      {
        error: Le.bind(0, {
          plugin: { type: "watcher", name: t.name },
          element: { id: e.target.id, tag: e.target.tagName },
        }),
      },
      e.detail.argsRaw,
    );
});
var He = (e) => {
    En.set(e.name, e);
  },
  wn = (e) => {
    for (let t of e) {
      let r = Ie.get(t);
      if (r && Ie.delete(t))
        for (let o of r.values()) for (let n of o.values()) n();
    }
  },
  xn = se("ignore"),
  $r = `[${xn}]`,
  kn = (e) => e.hasAttribute(`${xn}__self`) || !!e.closest($r),
  me = (e, t) => {
    for (let r of e)
      if (!kn(r)) {
        let o = new Set();
        for (let n in r.dataset) {
          let i = n.replace(/[A-Z]/g, "-$&").toLowerCase();
          o.add(i), Ft(r, i, r.dataset[n], t);
        }
        for (let n of Array.from(r.attributes)) {
          if (!n.name.startsWith("data-")) continue;
          let i = n.name.slice(5);
          o.has(i) || Ft(r, i, n.value, t);
        }
      }
  },
  Ir = (e) => {
    for (let {
      target: t,
      type: r,
      attributeName: o,
      addedNodes: n,
      removedNodes: i,
    } of e)
      if (r === "childList") {
        for (let s of i) oe(s) && (wn([s]), wn(s.querySelectorAll("*")));
        for (let s of n) oe(s) && (me([s]), me(s.querySelectorAll("*")));
      } else if (
        r === "attributes" &&
        o.startsWith("data-") &&
        oe(t) &&
        !kn(t)
      ) {
        let s = o.slice(5),
          a = Lt(s);
        if (!a) continue;
        let c = t.getAttribute(o);
        if (c === null) {
          let l = Ie.get(t);
          if (l) {
            let u = l.get(a);
            if (u) {
              for (let d of u.values()) d();
              l.delete(a);
            }
          }
        } else Ft(t, s, c);
      }
  },
  Nn = new MutationObserver(Ir),
  Fr = (e) => {
    let [t, ...r] = e.split("__"),
      [o, n] = t.split(/:(.+)/),
      i = new Map();
    for (let s of r) {
      let [a, ...c] = s.split(".");
      i.set(a, new Set(c));
    }
    return { pluginName: o, key: n, mods: i };
  },
  at = () => Se.has(document.documentElement),
  Pn = () => {
    An || !at() || ((An = !0), document.dispatchEvent(new Event(Ze)));
  },
  Hr = (e = document.documentElement, t = !0) => {
    oe(e) && me([e], !0),
      me(e.querySelectorAll("*"), !0),
      t &&
        (Nn.observe(e, { subtree: !0, childList: !0, attributes: !0 }),
        Se.add(e),
        Pn());
  },
  _e = (e = document.documentElement, t = !0) => {
    oe(e) && me([e]),
      me(e.querySelectorAll("*")),
      t &&
        (Nn.observe(e, { subtree: !0, childList: !0, attributes: !0 }),
        Se.add(e),
        Pn());
  },
  Mn = (e, t = !1) => {
    me([e], t);
  },
  Ft = (e, t, r, o) => {
    let n = Lt(t);
    if (!n) return;
    let { pluginName: i, key: s, mods: a } = Fr(n),
      c = $t.get(i);
    if ((!o || It.has(i)) && !!c) {
      let u = {
          el: e,
          rawKey: n,
          mods: a,
          error: Le.bind(0, {
            plugin: { type: "attribute", name: c.name },
            element: { id: e.id, tag: e.tagName },
            expression: { rawKey: n, key: s, value: r },
          }),
          key: s,
          value: r,
          loadedPluginNames: {
            actions: new Set($e.keys()),
            attributes: new Set($t.keys()),
          },
          rx: void 0,
        },
        d =
          (c.requirement &&
            (typeof c.requirement == "string"
              ? c.requirement
              : c.requirement.key)) ||
          "allowed",
        p =
          (c.requirement &&
            (typeof c.requirement == "string"
              ? c.requirement
              : c.requirement.value)) ||
          "allowed",
        b = s != null && s !== "",
        g = r != null && r !== "";
      if (b) {
        if (d === "denied") throw u.error("KeyNotAllowed");
      } else if (d === "must") throw u.error("KeyRequired");
      if (g) {
        if (p === "denied") throw u.error("ValueNotAllowed");
      } else if (p === "must") throw u.error("ValueRequired");
      if (d === "exclusive" || p === "exclusive") {
        if (b && g) throw u.error("KeyAndValueProvided");
        if (!b && !g) throw u.error("KeyOrValueRequired");
      }
      let m = new Map();
      if (g) {
        let x;
        u.rx = (...v) => (
          x ||
            (x = Re(r, {
              returnsValue: c.returnsValue,
              argNames: c.argNames,
              cleanups: m,
            })),
          x(e, ...v)
        );
      }
      let w = c.apply(u);
      w && m.set("attribute", w);
      let E = Ie.get(e);
      if (E) {
        let x = E.get(n);
        if (x) for (let v of x.values()) v();
      } else (E = new Map()), Ie.set(e, E);
      E.set(n, m);
    }
  },
  Re = (
    e,
    { returnsValue: t = !1, argNames: r = [], cleanups: o = new Map() } = {},
  ) => {
    let n = "";
    if (t) {
      let c =
          /(\/(\\\/|[^/])*\/|"(\\"|[^"])*"|'(\\'|[^'])*'|`(\\`|[^`])*`|\(\s*((function)\s*\(\s*\)|(\(\s*\))\s*=>)\s*(?:\{[\s\S]*?\}|[^;){]*)\s*\)\s*\(\s*\)|[^;])+/gm,
        l = e.trim().match(c);
      if (l) {
        let u = l.length - 1,
          d = l[u].trim();
        d.startsWith("return") || (l[u] = `return (${d});`),
          (n = l.join(`;
`));
      }
    } else n = e.trim();
    let i = new Map(),
      s = RegExp(`(?:${Et})(.*?)(?:${xt})`, "gm"),
      a = 0;
    for (let c of n.matchAll(s)) {
      let l = c[1],
        u = `__escaped${a++}`;
      i.set(u, l), (n = n.replace(Et + l + xt, u));
    }
    (n = n.replace(
      /("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\$]|\$(?!\{))*`)|\$\{([^{}]*)\}|\$([a-zA-Z_\d]\w*(?:[.-]\w+)*)/g,
      (c, l, u, d) =>
        l
          ? c
          : u !== void 0
            ? `\${${u.replace(/\$([a-zA-Z_\d]\w*(?:[.-]\w+)*)/g, (p, b) => b.split(".").reduce((g, m) => `${g}['${m}']`, "$"))}}`
            : d.split(".").reduce((p, b) => `${p}['${b}']`, "$"),
    )),
      (n = n.replaceAll(/@([A-Za-z_$][\w$]*)\(/g, '__action("$1",evt,'));
    for (let [c, l] of i) n = n.replace(c, l);
    try {
      let c = Function("el", "$", "__action", "evt", ...r, n);
      return (l, ...u) => {
        let d = (p, b, ...g) => {
          let m = Le.bind(0, {
              plugin: { type: "action", name: p },
              element: { id: l.id, tag: l.tagName },
              expression: { fnContent: n, value: e },
            }),
            w = Fe[p];
          if (w) return w({ el: l, evt: b, error: m, cleanups: o }, ...g);
          throw m("UndefinedAction");
        };
        try {
          return c(l, Z, d, void 0, ...u);
        } catch (p) {
          throw (
            (console.error(p),
            Le(
              {
                element: { id: l.id, tag: l.tagName },
                expression: { fnContent: n, value: e },
                error: p.message,
              },
              "ExecuteExpression",
            ))
          );
        }
      };
    } catch (c) {
      throw (
        (console.error(c),
        Le(
          { expression: { fnContent: n, value: e }, error: c.message },
          "GenerateExpression",
        ))
      );
    }
  };
L({
  name: "peek",
  apply(e, t) {
    V();
    try {
      return t();
    } finally {
      q();
    }
  },
});
L({
  name: "setAll",
  apply(e, t, r) {
    V();
    let o = B(r);
    de(o, () => t), U(o), q();
  },
});
L({
  name: "toggleAll",
  apply(e, t) {
    V();
    let r = B(t);
    de(r, (o) => !o), U(r), q();
  },
});
var On = new WeakMap(),
  Ht = (e) => !["GET", "DELETE"].includes(e),
  Ve = (e, t, r = !0) =>
    L({
      name: e,
      apply: async (
        { el: o, evt: n, error: i, cleanups: s },
        a,
        {
          selector: c,
          headers: l,
          contentType: u = "json",
          filterSignals: { include: d = /.*/, exclude: p = /(^|\.)_/ } = {},
          openWhenHidden: b = r,
          payload: g,
          requestCancellation: m = "auto",
          retry: w = "auto",
          retryInterval: E = 1e3,
          retryScaler: x = 2,
          retryMaxWait: v = 3e4,
          retryMaxCount: f = 10,
        } = {},
      ) => {
        let h = m instanceof AbortController ? m : new AbortController();
        (m === "auto" || m === "cleanup") && (On.get(o)?.abort(), On.set(o, h)),
          m === "cleanup" &&
            (s.get(`@${e}`)?.(),
            s.set(`@${e}`, async () => {
              h.abort(), await Promise.resolve();
            }));
        let S = () => {};
        try {
          if (!a?.length) throw i("FetchNoUrlProvided", { action: L });
          let R = {
            Accept: "text/event-stream, text/html, application/json",
            "Datastar-Request": !0,
          };
          u === "json" && Ht(t) && (R["Content-Type"] = "application/json");
          let k = Object.assign({}, R, l),
            T = {
              input: "",
              method: t,
              headers: k,
              openWhenHidden: b,
              retry: w,
              retryInterval: E,
              retryScaler: x,
              retryMaxWait: v,
              retryMaxCount: f,
              signal: h.signal,
              onopen: async (C) => {
                C.status >= 400 && ge(_r, o, { status: C.status.toString() });
              },
              onmessage: (C) => {
                if (!C.event.startsWith("datastar")) return;
                let O = C.event,
                  P = {};
                for (let $ of C.data.split(`
`)) {
                  let I = $.indexOf(" "),
                    K = $.slice(0, I),
                    j = $.slice(I + 1);
                  (P[K] ||= []).push(j);
                }
                let D = Object.fromEntries(
                  Object.entries(P).map(([$, I]) => [
                    $,
                    I.join(`
`),
                  ]),
                );
                ge(O, o, D);
              },
              onerror: (C) => {
                if (Dn(C)) throw C("FetchExpectedTextEventStream", { url: a });
                C &&
                  (console.error(C.message), ge(Vr, o, { message: C.message }));
              },
            },
            y = () => {
              let C = new URL(a, document.baseURI),
                O = new URLSearchParams(C.search);
              if (u === "json") {
                V();
                let P = g !== void 0 ? g : B({ include: d, exclude: p });
                q();
                let D = JSON.stringify(P);
                Ht(t) ? (T.body = D) : O.set("datastar", D);
              } else if (u === "form") {
                let P = c ? document.querySelector(c) : o.closest("form");
                if (!P)
                  throw i("FetchFormNotFound", { action: L, selector: c });
                if (!P.noValidate && !P.checkValidity()) {
                  P.reportValidity();
                  return;
                }
                let D = new FormData(P),
                  $ = o;
                if (o === P && n instanceof SubmitEvent) $ = n.submitter;
                else {
                  let j = (Ne) => Ne.preventDefault();
                  P.addEventListener("submit", j),
                    (S = () => {
                      P.removeEventListener("submit", j);
                    });
                }
                if (
                  $ instanceof HTMLButtonElement ||
                  ($ instanceof HTMLInputElement && $.type === "submit")
                ) {
                  let j = $.getAttribute("name");
                  j && D.append(j, $.value);
                }
                let I = P.getAttribute("enctype") === "multipart/form-data";
                I || (k["Content-Type"] = "application/x-www-form-urlencoded");
                let K = new URLSearchParams(D);
                if (Ht(t)) I ? (T.body = D) : (T.body = K);
                else for (let [j, Ne] of K) O.append(j, Ne);
              } else
                throw i("FetchInvalidContentType", {
                  action: L,
                  contentType: u,
                });
              return (C.search = O.toString()), (T.input = C.toString()), T;
            };
          ge(_t, o, {});
          try {
            await Kr(o, y);
          } catch (C) {
            if (!Dn(C))
              throw i("FetchFailed", { method: t, url: a, error: C.message });
          }
        } finally {
          ge(Vt, o, {}), S(), s.delete(`@${e}`);
        }
      },
    });
Ve("get", "GET", !1);
Ve("patch", "PATCH");
Ve("post", "POST");
Ve("put", "PUT");
Ve("delete", "DELETE");
var _t = "started",
  Vt = "finished",
  _r = "error",
  Vr = "retrying",
  qr = "retries-failed",
  ge = (e, t, r) =>
    document.dispatchEvent(
      new CustomEvent(ne, { detail: { type: e, el: t, argsRaw: r } }),
    ),
  Dn = (e) => `${e}`.includes("text/event-stream"),
  jr = async (e, t) => {
    let r = e.getReader(),
      o = await r.read();
    for (; !o.done; ) t(o.value), (o = await r.read());
  },
  Br = (e) => {
    let t,
      r,
      o,
      n = !1;
    return (i) => {
      t ? (t = Ur(t, i)) : ((t = i), (r = 0), (o = -1));
      let s = t.length,
        a = 0;
      for (; r < s; ) {
        n && (t[r] === 10 && (a = ++r), (n = !1));
        let c = -1;
        for (; r < s && c === -1; ++r)
          switch (t[r]) {
            case 58:
              o === -1 && (o = r - a);
              break;
            case 13:
              n = !0;
            case 10:
              c = r;
              break;
          }
        if (c === -1) break;
        e(t.subarray(a, c), o), (a = r), (o = -1);
      }
      a === s ? (t = void 0) : a && ((t = t.subarray(a)), (r -= a));
    };
  },
  Wr = (e, t, r) => {
    let o = Ln(),
      n = new TextDecoder();
    return (i, s) => {
      if (!i.length) r?.(o), (o = Ln());
      else if (s > 0) {
        let a = n.decode(i.subarray(0, s)),
          c = s + (i[s + 1] === 32 ? 2 : 1),
          l = n.decode(i.subarray(c));
        switch (a) {
          case "data":
            o.data = o.data
              ? `${o.data}
${l}`
              : l;
            break;
          case "event":
            o.event = l;
            break;
          case "id":
            e((o.id = l));
            break;
          case "retry": {
            let u = +l;
            Number.isNaN(u) || t((o.retry = u));
            break;
          }
        }
      }
    };
  },
  Ur = (e, t) => {
    let r = new Uint8Array(e.length + t.length);
    return r.set(e), r.set(t, e.length), r;
  },
  Ln = () => ({ data: "", event: "", id: "", retry: void 0 }),
  Kr = (e, t) =>
    new Promise((r, o) => {
      let n = t();
      if (!n) return;
      let {
          input: i,
          signal: s,
          headers: a,
          onopen: c,
          onmessage: l,
          onclose: u,
          onerror: d,
          openWhenHidden: p,
          fetch: b,
          retry: g = "auto",
          retryInterval: m = 1e3,
          retryScaler: w = 2,
          retryMaxWait: E = 3e4,
          retryMaxCount: x = 10,
          responseOverrides: v,
          ...f
        } = n,
        h = { ...a },
        S,
        R = () => {
          if ((S.abort(), !document.hidden)) {
            let $ = t();
            if (!$) return;
            (i = $.input), (f.body = $.body), D();
          }
        };
      p || document.addEventListener("visibilitychange", R);
      let k,
        T = () => {
          document.removeEventListener("visibilitychange", R),
            clearTimeout(k),
            S.abort();
        };
      s?.addEventListener("abort", () => {
        T(), r();
      });
      let y = b || window.fetch,
        C = c || (() => {}),
        O = 0,
        P = m,
        D = async () => {
          S = new AbortController();
          let $ = S.signal;
          try {
            let I = await y(i, { ...f, headers: h, signal: $ });
            await C(I);
            let K = async (Y, Pe, Ct, Je, ...kr) => {
                let ln = { [Ct]: await Pe.text() };
                for (let At of kr) {
                  let wt = Pe.headers.get(`datastar-${ee(At)}`);
                  if (Je) {
                    let ze = Je[At];
                    ze &&
                      (wt = typeof ze == "string" ? ze : JSON.stringify(ze));
                  }
                  wt && (ln[At] = wt);
                }
                ge(Y, e, ln), T(), r();
              },
              j = I.status,
              Ne = j === 204,
              cn = j >= 300 && j < 400,
              xr = j >= 400 && j < 600;
            if (j !== 200) {
              if (
                (u?.(),
                g !== "never" &&
                  !Ne &&
                  !cn &&
                  (g === "always" || (g === "error" && xr)))
              ) {
                clearTimeout(k), (k = setTimeout(D, m));
                return;
              }
              T(), r();
              return;
            }
            (O = 0), (m = P);
            let Rt = I.headers.get("Content-Type");
            if (Rt?.includes("text/html"))
              return await K(
                "datastar-patch-elements",
                I,
                "elements",
                v,
                "selector",
                "mode",
                "namespace",
                "useViewTransition",
              );
            if (Rt?.includes("application/json"))
              return await K(
                "datastar-patch-signals",
                I,
                "signals",
                v,
                "onlyIfMissing",
              );
            if (Rt?.includes("text/javascript")) {
              let Y = document.createElement("script"),
                Pe = I.headers.get("datastar-script-attributes");
              if (Pe)
                for (let [Ct, Je] of Object.entries(JSON.parse(Pe)))
                  Y.setAttribute(Ct, Je);
              (Y.textContent = await I.text()),
                document.head.appendChild(Y),
                T();
              return;
            }
            if (
              (await jr(
                I.body,
                Br(
                  Wr(
                    (Y) => {
                      Y ? (h["last-event-id"] = Y) : delete h["last-event-id"];
                    },
                    (Y) => {
                      P = m = Y;
                    },
                    l,
                  ),
                ),
              ),
              u?.(),
              g === "always" && !cn)
            ) {
              clearTimeout(k), (k = setTimeout(D, m));
              return;
            }
            T(), r();
          } catch (I) {
            if (!$.aborted)
              try {
                let K = d?.(I) || m;
                clearTimeout(k),
                  (k = setTimeout(D, K)),
                  (m = Math.min(m * w, E)),
                  ++O >= x
                    ? (ge(qr, e, {}), T(), o("Max retries reached."))
                    : console.error(
                        `Datastar failed to reach ${i.toString()} retrying in ${K}ms.`,
                      );
              } catch (K) {
                T(), o(K);
              }
          }
        };
      D();
    });
A({
  name: "attr",
  requirement: { value: "must" },
  returnsValue: !0,
  apply({ el: e, key: t, rx: r }) {
    let o = (a, c) => {
        c === "" || c === !0
          ? e.setAttribute(a, "")
          : c === !1 || c == null
            ? e.removeAttribute(a)
            : typeof c == "string"
              ? e.setAttribute(a, c)
              : typeof c == "function"
                ? e.setAttribute(a, c.toString())
                : e.setAttribute(
                    a,
                    JSON.stringify(c, (l, u) =>
                      typeof u == "function" ? u.toString() : u,
                    ),
                  );
      },
      n = t
        ? () => {
            i.disconnect();
            let a = r();
            o(t, a), i.observe(e, { attributeFilter: [t] });
          }
        : () => {
            i.disconnect();
            let a = r(),
              c = Object.keys(a);
            for (let l of c) o(l, a[l]);
            i.observe(e, { attributeFilter: c });
          },
      i = new MutationObserver(n),
      s = M(n);
    return () => {
      i.disconnect(), s();
    };
  },
});
var ct = (e, ...t) => ({
    get: (r) => r[e],
    set: (r, o) => {
      r[e] = o;
    },
    events: t,
  }),
  $n = (e, ...t) => ({
    get: (r) => r.getAttribute(e),
    set: (r, o) => {
      r.setAttribute(e, `${o}`);
    },
    events: t,
  }),
  qt = (e = !1, ...t) => ({
    get: (r, o) =>
      o === "string" || (e && o === "undefined") ? r.value : +r.value,
    set: (r, o) => {
      r.value = `${o}`;
    },
    events: t,
  }),
  Gr = /^data:(?<mime>[^;]+);base64,(?<contents>.*)$/,
  In = Symbol("empty"),
  lt = se("bind"),
  Jr = (e, t, r, o, n, i) => {
    if (i === void 0 && e instanceof HTMLInputElement && e.type === "radio") {
      let u = t || r,
        d = [
          ...document.querySelectorAll(
            `[${lt}\\:${CSS.escape(u)}],[${lt}="${CSS.escape(u)}"]`,
          ),
        ].find((p) => p instanceof HTMLInputElement && p.checked);
      d && N([[o, d.value]], { ifMissing: !0 });
    }
    if (!Array.isArray(i) || (e instanceof HTMLSelectElement && e.multiple))
      return N([[o, n.get(e, typeof i)]], { ifMissing: !0 }), o;
    let s = t || r,
      a = document.querySelectorAll(
        `[${lt}\\:${CSS.escape(s)}],[${lt}="${CSS.escape(s)}"]`,
      ),
      c = [],
      l = 0;
    for (let u of a) {
      if (
        (c.push([`${o}.${l}`, n.get(u, typeof (W(i, l) ? i[l] : void 0))]),
        e === u)
      )
        break;
      l++;
    }
    return N(c, { ifMissing: !0 }), `${o}.${l}`;
  };
A({
  name: "bind",
  requirement: "exclusive",
  apply({ el: e, key: t, mods: r, value: o, error: n }) {
    let i = t != null ? _(t, r) : o,
      s = r.get("prop"),
      a = r.get("event"),
      c = null;
    if (e instanceof HTMLInputElement)
      switch (e.type) {
        case "range":
        case "number":
          c = qt(!1, "input");
          break;
        case "checkbox":
          c = {
            get: (g, m) =>
              g.value !== "on"
                ? m === "boolean"
                  ? g.checked
                  : g.checked
                    ? g.value
                    : ""
                : m === "string"
                  ? g.checked
                    ? g.value
                    : ""
                  : g.checked,
            set: (g, m) => {
              g.checked = typeof m == "string" ? m === g.value : m;
            },
            events: ["change"],
          };
          break;
        case "radio":
          e.getAttribute("name")?.length || e.setAttribute("name", i),
            (c = {
              get: (g, m) =>
                g.checked ? (m === "number" ? +g.value : g.value) : In,
              set: (g, m) => {
                g.checked = m === (typeof m == "number" ? +g.value : g.value);
              },
              events: ["change"],
            });
          break;
        case "file": {
          let g = () => {
            let m = [...(e.files || [])],
              w = [];
            Promise.all(
              m.map(
                (E) =>
                  new Promise((x) => {
                    let v = new FileReader();
                    (v.onload = () => {
                      if (typeof v.result != "string")
                        throw n("InvalidFileResultType", {
                          resultType: typeof v.result,
                        });
                      let f = v.result.match(Gr);
                      if (!f?.groups)
                        throw n("InvalidDataUri", { result: v.result });
                      w.push({
                        name: E.name,
                        contents: f.groups.contents,
                        mime: f.groups.mime,
                      });
                    }),
                      (v.onloadend = () => x()),
                      v.readAsDataURL(E);
                  }),
              ),
            ).then(() => {
              N([[i, w]]);
            });
          };
          return (
            e.addEventListener("change", g),
            () => {
              e.removeEventListener("change", g);
            }
          );
        }
        default:
          c = qt(!0, "input");
      }
    else if (e instanceof HTMLSelectElement && e.multiple) {
      let g = new Map();
      c = {
        get: (m) =>
          [...m.selectedOptions].map((w) => {
            let E = g.get(w.value);
            return E === "string" || E == null ? w.value : +w.value;
          }),
        set: (m, w) => {
          for (let E of m.options)
            w.includes(E.value)
              ? (g.set(E.value, "string"), (E.selected = !0))
              : w.includes(+E.value)
                ? (g.set(E.value, "number"), (E.selected = !0))
                : (E.selected = !1);
        },
        events: ["change"],
      };
    } else
      e instanceof HTMLSelectElement
        ? (c = qt(!0, "change"))
        : e instanceof HTMLTextAreaElement
          ? (c = ct("value", "input"))
          : e instanceof HTMLElement && e.tagName.includes("-")
            ? (c =
                "value" in e
                  ? ct("value", "input", "change")
                  : $n("value", "input", "change"))
            : e instanceof HTMLElement && "value" in e
              ? (c = ct("value", "change"))
              : (c = $n("value", "change"));
    if (!c) throw n("InvalidBindAdapter");
    let l = s && [...s][0];
    if (s && !l) throw n("BindPropNameMissing");
    if (l) {
      let g = Rn(l);
      c = ct(g, ...(a ? [...a] : c.events));
    } else a && (c.events = [...a]);
    let u = X(i),
      d = Jr(e, t, o, i, c, u),
      p = () => {
        let g = X(d);
        if (g != null) {
          let m = c.get(e, typeof g);
          m !== In && N([[d, m]]);
        }
      };
    for (let g of c.events) e.addEventListener(g, p);
    e.addEventListener(Me, p);
    let b = M(() => {
      c.set(e, X(d));
    });
    return () => {
      b();
      for (let g of c.events) e.removeEventListener(g, p);
      e.removeEventListener(Me, p);
    };
  },
});
A({
  name: "class",
  requirement: { value: "must" },
  returnsValue: !0,
  apply({ key: e, el: t, mods: r, rx: o }) {
    e &&= _(e, r, "kebab");
    let n,
      i = () => {
        s.disconnect(), (n = e ? { [e]: o() } : o());
        for (let c in n) {
          let l = c.split(/\s+/).filter((u) => u.length > 0);
          if (n[c])
            for (let u of l) t.classList.contains(u) || t.classList.add(u);
          else
            for (let u of l) t.classList.contains(u) && t.classList.remove(u);
        }
        s.observe(t, { attributeFilter: ["class"] });
      },
      s = new MutationObserver(i),
      a = M(i);
    return () => {
      s.disconnect(), a();
      for (let c in n) {
        let l = c.split(/\s+/).filter((u) => u.length > 0);
        for (let u of l) t.classList.remove(u);
      }
    };
  },
});
A({
  name: "computed",
  requirement: { value: "must" },
  returnsValue: !0,
  apply({ key: e, mods: t, rx: r, error: o }) {
    if (e) N([[_(e, t), pe(r)]]);
    else {
      let n = Object.assign({}, r());
      de(n, (i) => {
        if (typeof i == "function") return pe(i);
        throw o("ComputedExpectedFunction");
      }),
        U(n);
    }
  },
});
A({
  name: "effect",
  requirement: { key: "denied", value: "must" },
  apply: ({ rx: e }) => M(e),
});
A({
  name: "indicator",
  requirement: "exclusive",
  apply({ el: e, key: t, mods: r, value: o }) {
    let n = t != null ? _(t, r) : o,
      i = 0;
    N([[n, !1]]);
    let s = (a) => {
      let { type: c, el: l } = a.detail;
      if (l === e)
        switch (c) {
          case _t:
            i++, N([[n, !0]]);
            break;
          case Vt:
            (i = Math.max(0, i - 1)), N([[n, i > 0]]);
            break;
        }
    };
    return (
      document.addEventListener(ne, s),
      () => {
        (i = 0), N([[n, !1]]), document.removeEventListener(ne, s);
      }
    );
  },
});
var Q = (e) => {
    if (!e || e.size <= 0) return 0;
    for (let t of e) {
      if (t.endsWith("ms")) return +t.replace("ms", "");
      if (t.endsWith("s")) return +t.replace("s", "") * 1e3;
      try {
        return Number.parseFloat(t);
      } catch {}
    }
    return 0;
  },
  he = (e, t, r = !1) => (e ? e.has(t.toLowerCase()) : r),
  ut = (e, t = "") => {
    if (e && e.size > 0) for (let r of e) return r;
    return t;
  };
var jt =
    (e, t) =>
    (...r) => {
      setTimeout(() => {
        e(...r);
      }, t);
    },
  Fn = (e, t, r = !0, o = !1, n = !1) => {
    let i = null,
      s = 0;
    return (...a) => {
      r && !s ? (e(...a), (i = null)) : (i = a),
        (!s || n) &&
          (s && clearTimeout(s),
          (s = setTimeout(() => {
            o && i !== null && e(...i), (i = null), (s = 0);
          }, t)));
    };
  },
  te = (e, t) => {
    let r = t.get("delay");
    if (r) {
      let i = Q(r);
      e = jt(e, i);
    }
    let o = t.get("debounce");
    if (o) {
      let i = Q(o),
        s = he(o, "leading", !1),
        a = !he(o, "notrailing", !1);
      e = Fn(e, i, s, a, !0);
    }
    let n = t.get("throttle");
    if (n) {
      let i = Q(n),
        s = !he(n, "noleading", !1),
        a = he(n, "trailing", !1);
      e = Fn(e, i, s, a);
    }
    return e;
  };
var qe = !!document.startViewTransition,
  G = (e, t) => {
    if (t.has("viewtransition") && qe) {
      let r = e;
      e = (...o) => document.startViewTransition(() => r(...o));
    }
    return e;
  };
A({
  name: "init",
  requirement: { key: "denied", value: "must" },
  apply({ rx: e, mods: t }) {
    let r = () => {
      F(), e(), H();
    };
    r = G(r, t);
    let o = 0,
      n = t.get("delay");
    n && ((o = Q(n)), o > 0 && (r = jt(r, o))), r();
  },
});
A({
  name: "json-signals",
  requirement: { key: "denied" },
  apply({ el: e, value: t, mods: r }) {
    let o = r.has("terse") ? 0 : 2,
      n = {};
    t && (n = le(t));
    let i = () => {
        s.disconnect(),
          (e.textContent = JSON.stringify(B(n), null, o)),
          s.observe(e, { childList: !0, characterData: !0, subtree: !0 });
      },
      s = new MutationObserver(i),
      a = M(i);
    return () => {
      s.disconnect(), a();
    };
  },
});
A({
  name: "on",
  requirement: "must",
  argNames: ["evt"],
  apply({ el: e, key: t, mods: r, rx: o }) {
    let n = e;
    r.has("window") ? (n = window) : r.has("document") && (n = document);
    let i = (l) => {
      F(), o(l), H();
    };
    (i = G(i, r)), (i = te(i, r));
    let s = _(t, r, "kebab"),
      a = {
        capture: r.has("capture"),
        passive: r.has("passive"),
        once: r.has("once"),
      };
    if (r.has("outside")) {
      n = document;
      let l = i;
      i = (u) => {
        e.contains(u?.target) || l(u);
      };
    }
    (s === ne || s === fe) && (n = document);
    let c = (l) => {
      l &&
        (r.has("prevent") && l.preventDefault(),
        r.has("stop") && l.stopPropagation(),
        e instanceof HTMLFormElement && s === "submit" && l.preventDefault()),
        i(l);
    };
    return (
      n.addEventListener(s, c, a),
      () => {
        n.removeEventListener(s, c, a);
      }
    );
  },
});
var ft = (e, t, r) => Math.max(t, Math.min(r, e)),
  dt = (e, t, r, o = !0) => {
    let n = e + (t - e) * r;
    return o ? ft(n, e, t) : n;
  },
  pt = (e, t, r, o = !0) => {
    if (r < e) return 0;
    if (r > t) return 1;
    let n = (r - e) / (t - e);
    return o ? ft(n, e, t) : n;
  };
var Bt = new WeakSet();
A({
  name: "on-intersect",
  requirement: { key: "denied", value: "must" },
  apply({ el: e, mods: t, rx: r }) {
    let o = () => {
      F(), r(), H();
    };
    (o = G(o, t)), (o = te(o, t));
    let n = { threshold: 0 };
    if (t.has("full")) n.threshold = 1;
    else if (t.has("half")) n.threshold = 0.5;
    else {
      let a = t.get("threshold");
      a && (n.threshold = ft(Number(ut(a)), 0, 100) / 100);
    }
    let i = t.has("exit"),
      s = new IntersectionObserver((a) => {
        for (let c of a)
          c.isIntersecting !== i && (o(), s && Bt.has(e) && s.disconnect());
      }, n);
    return (
      s.observe(e),
      t.has("once") && Bt.add(e),
      () => {
        t.has("once") || Bt.delete(e), s && (s.disconnect(), (s = null));
      }
    );
  },
});
A({
  name: "on-interval",
  requirement: { key: "denied", value: "must" },
  apply({ mods: e, rx: t }) {
    let r = () => {
      F(), t(), H();
    };
    r = G(r, e);
    let o = 1e3,
      n = e.get("duration");
    n && ((o = Q(n)), he(n, "leading", !1) && r());
    let i = setInterval(r, o);
    return () => {
      clearInterval(i);
    };
  },
});
A({
  name: "on-signal-patch",
  requirement: { value: "must" },
  argNames: ["patch"],
  returnsValue: !0,
  apply({ el: e, key: t, mods: r, rx: o, error: n }) {
    if (t && t !== "filter") throw n("KeyNotAllowed");
    let i = se(`${this.name}-filter`),
      s = e.getAttribute(i),
      a = {};
    s && (a = le(s));
    let c = !1,
      l = te((u) => {
        if (c) return;
        let d = B(a, u.detail);
        if (!Qe(d)) {
          (c = !0), F();
          try {
            o(d);
          } finally {
            H(), (c = !1);
          }
        }
      }, r);
    return (
      document.addEventListener(fe, l),
      () => {
        document.removeEventListener(fe, l);
      }
    );
  },
});
A({
  name: "ref",
  requirement: "exclusive",
  apply({ el: e, key: t, mods: r, value: o }) {
    let n = t != null ? _(t, r) : o;
    N([[n, e]]);
  },
});
var Hn = "none",
  _n = "display";
A({
  name: "show",
  requirement: { key: "denied", value: "must" },
  returnsValue: !0,
  apply({ el: e, rx: t }) {
    let r = () => {
        o.disconnect(),
          t()
            ? e.style.display === Hn && e.style.removeProperty(_n)
            : e.style.setProperty(_n, Hn),
          o.observe(e, { attributeFilter: ["style"] });
      },
      o = new MutationObserver(r),
      n = M(r);
    return () => {
      o.disconnect(), n();
    };
  },
});
A({
  name: "signals",
  returnsValue: !0,
  apply({ key: e, mods: t, rx: r }) {
    let o = t.has("ifmissing");
    if (e) {
      e = _(e, t);
      let n = r?.();
      N([[e, n]], { ifMissing: o });
    } else {
      let n = Object.assign({}, r?.());
      U(n, { ifMissing: o });
    }
  },
});
A({
  name: "style",
  requirement: { value: "must" },
  returnsValue: !0,
  apply({ key: e, el: t, rx: r }) {
    let { style: o } = t,
      n = new Map(),
      i = (l, u) => {
        let d = n.get(l);
        !u && u !== 0
          ? d !== void 0 && (d ? o.setProperty(l, d) : o.removeProperty(l))
          : (d === void 0 && n.set(l, o.getPropertyValue(l)),
            o.setProperty(l, String(u)));
      },
      s = () => {
        if ((a.disconnect(), e)) i(e, r());
        else {
          let l = r();
          for (let [u, d] of n)
            u in l || (d ? o.setProperty(u, d) : o.removeProperty(u));
          for (let u in l) i(ee(u), l[u]);
        }
        a.observe(t, { attributeFilter: ["style"] });
      },
      a = new MutationObserver(s),
      c = M(s);
    return () => {
      a.disconnect(), c();
      for (let [l, u] of n) u ? o.setProperty(l, u) : o.removeProperty(l);
    };
  },
});
A({
  name: "text",
  requirement: { key: "denied", value: "must" },
  returnsValue: !0,
  apply({ el: e, rx: t }) {
    let r = () => {
        o.disconnect(),
          (e.textContent = `${t()}`),
          o.observe(e, { childList: !0, characterData: !0, subtree: !0 });
      },
      o = new MutationObserver(r),
      n = M(r);
    return () => {
      o.disconnect(), n();
    };
  },
});
var Vn = (e, t) => e.includes(t),
  zr = [
    "remove",
    "outer",
    "inner",
    "replace",
    "prepend",
    "append",
    "before",
    "after",
  ],
  Zr = ["html", "svg", "mathml"];
He({
  name: "datastar-patch-elements",
  apply(e, t) {
    let r = typeof t.selector == "string" ? t.selector : "",
      o = typeof t.mode == "string" ? t.mode : "outer",
      n = typeof t.namespace == "string" ? t.namespace : "html",
      i = typeof t.useViewTransition == "string" ? t.useViewTransition : "",
      s = t.elements;
    if (!Vn(zr, o)) throw e.error("PatchElementsInvalidMode", { mode: o });
    if (!r && o !== "outer" && o !== "replace")
      throw e.error("PatchElementsExpectedSelector");
    if (!Vn(Zr, n))
      throw e.error("PatchElementsInvalidNamespace", { namespace: n });
    let a = {
      selector: r,
      mode: o,
      namespace: n,
      useViewTransition: i.trim() === "true",
      elements: s,
    };
    qe && a.useViewTransition
      ? document.startViewTransition(() => qn(e, a))
      : qn(e, a);
  },
});
var qn = (
    { error: e },
    { selector: t, mode: r, namespace: o, elements: n },
  ) => {
    let i = document.createDocumentFragment(),
      s = typeof n != "string" && !!n;
    if (typeof n == "string") {
      let a = n.replace(/<svg(\s[^>]*>|>)([\s\S]*?)<\/svg>/gim, ""),
        c = /<\/html>/.test(a),
        l = /<\/head>/.test(a),
        u = /<\/body>/.test(a),
        d = o === "svg" ? "svg" : o === "mathml" ? "math" : "",
        p = d ? `<${d}>${n}</${d}>` : n,
        b = new DOMParser().parseFromString(
          c || l || u ? n : `<body><template>${p}</template></body>`,
          "text/html",
        );
      if (c) i.appendChild(b.documentElement);
      else if (l && u) i.appendChild(b.head), i.appendChild(b.body);
      else if (l) i.appendChild(b.head);
      else if (u) i.appendChild(b.body);
      else if (d) {
        let g = b.querySelector("template").content.querySelector(d);
        for (let m of g.childNodes) i.appendChild(m);
      } else i = b.querySelector("template").content;
    } else
      n &&
        (n instanceof DocumentFragment
          ? (i = n)
          : n instanceof Element && i.appendChild(n));
    if (!t && (r === "outer" || r === "replace")) {
      let a = Array.from(i.children);
      for (let c of a) {
        let l;
        if (c instanceof HTMLHtmlElement) l = document.documentElement;
        else if (c instanceof HTMLBodyElement) l = document.body;
        else if (c instanceof HTMLHeadElement) l = document.head;
        else if (((l = document.getElementById(c.id)), !l)) {
          console.warn(e("PatchElementsNoTargetsFound"), {
            element: { id: c.id },
          });
          continue;
        }
        Bn(r, c, [l], s);
      }
    } else {
      let a = document.querySelectorAll(t);
      if (!a.length) {
        console.warn(e("PatchElementsNoTargetsFound"), { selector: t });
        return;
      }
      let c = s && r !== "remove" ? [a[0]] : a;
      Bn(r, i, c, s);
    }
  },
  Ut = new WeakSet();
for (let e of document.querySelectorAll("script")) Ut.add(e);
var Gn = (e) => {
    let t = e instanceof HTMLScriptElement ? [e] : e.querySelectorAll("script");
    for (let r of t)
      if (!Ut.has(r)) {
        let o = document.createElement("script");
        for (let { name: n, value: i } of r.attributes) o.setAttribute(n, i);
        (o.text = r.text), r.replaceWith(o), Ut.add(o);
      }
  },
  jn = (e, t, r, o) => {
    let n = !1;
    for (let i of e) {
      if (o && n) break;
      let s = o ? t : t.cloneNode(!0);
      Gn(s), i[r](s), (n = !0);
    }
  },
  Bn = (e, t, r, o) => {
    switch (e) {
      case "remove":
        for (let n of r) n.remove();
        break;
      case "outer":
      case "inner":
        {
          let n = !1;
          for (let i of r) {
            if (o && n) break;
            let s = o ? t : t.cloneNode(!0);
            Kt(i, s, e), Gn(i);
            let a = i.closest("[data-scope-children]");
            a && a.dispatchEvent(new CustomEvent(ve, { bubbles: !1 })),
              (n = !0);
          }
        }
        break;
      case "replace":
        jn(r, t, "replaceWith", o);
        break;
      case "prepend":
      case "append":
      case "before":
      case "after":
        jn(r, t, e, o);
    }
  },
  J = new Map(),
  Ae = new Set(),
  Ce = new Map(),
  je = new Set(),
  mt = document.createElement("div");
mt.hidden = !0;
var Be = se("ignore-morph"),
  Qr = `[${Be}]`,
  Kt = (e, t, r = "outer") => {
    if (
      (oe(e) && oe(t) && e.hasAttribute(Be) && t.hasAttribute(Be)) ||
      e.parentElement?.closest(Qr)
    )
      return;
    let o = document.createElement("div");
    o.append(t), document.body.insertAdjacentElement("afterend", mt);
    let n = e.querySelectorAll("[id]");
    for (let { id: a, tagName: c } of n) Ce.has(a) ? je.add(a) : Ce.set(a, c);
    e instanceof Element &&
      e.id &&
      (Ce.has(e.id) ? je.add(e.id) : Ce.set(e.id, e.tagName)),
      Ae.clear();
    let i = o.querySelectorAll("[id]");
    for (let { id: a, tagName: c } of i)
      Ae.has(a) ? je.add(a) : Ce.get(a) === c && Ae.add(a);
    for (let a of je) Ae.delete(a);
    Ce.clear(), je.clear(), J.clear();
    let s = r === "outer" ? e.parentElement : e;
    Kn(s, n),
      Kn(o, i),
      Jn(s, o, r === "outer" ? e : null, e.nextSibling),
      mt.remove();
  },
  Jn = (e, t, r = null, o = null) => {
    e instanceof HTMLTemplateElement &&
      t instanceof HTMLTemplateElement &&
      ((e = e.content), (t = t.content)),
      (r ??= e.firstChild);
    for (let n of t.childNodes) {
      if (r && r !== o) {
        let i = Yr(n, r, o);
        if (i) {
          if (i !== r) {
            let s = r;
            for (; s && s !== i; ) {
              let a = s;
              (s = s.nextSibling), Un(a);
            }
          }
          Wt(i, n), (r = i.nextSibling);
          continue;
        }
      }
      if (n instanceof Element && Ae.has(n.id)) {
        let i = document.getElementById(n.id),
          s = i;
        for (; (s = s.parentNode); ) {
          let a = J.get(s);
          a && (a.delete(n.id), a.size || J.delete(s));
        }
        zn(e, i, r), Wt(i, n), (r = i.nextSibling);
        continue;
      }
      if (J.has(n)) {
        let i = n.namespaceURI,
          s = n.tagName,
          a =
            i && i !== "http://www.w3.org/1999/xhtml"
              ? document.createElementNS(i, s)
              : document.createElement(s);
        e.insertBefore(a, r), Wt(a, n), (r = a.nextSibling);
      } else {
        let i = document.importNode(n, !0);
        e.insertBefore(i, r), (r = i.nextSibling);
      }
    }
    for (; r && r !== o; ) {
      let n = r;
      (r = r.nextSibling), Un(n);
    }
  },
  Yr = (e, t, r) => {
    let o = null,
      n = e.nextSibling,
      i = 0,
      s = 0,
      a = J.get(e)?.size || 0,
      c = t;
    for (; c && c !== r; ) {
      if (Wn(c, e)) {
        let l = !1,
          u = J.get(c),
          d = J.get(e);
        if (d && u) {
          for (let p of u)
            if (d.has(p)) {
              l = !0;
              break;
            }
        }
        if (l) return c;
        if (!o && !J.has(c)) {
          if (!a) return c;
          o = c;
        }
      }
      if (((s += J.get(c)?.size || 0), s > a)) break;
      o === null &&
        n &&
        Wn(c, n) &&
        (i++, (n = n.nextSibling), i >= 2 && (o = void 0)),
        (c = c.nextSibling);
    }
    return o || null;
  },
  Wn = (e, t) =>
    e.nodeType === t.nodeType &&
    e.tagName === t.tagName &&
    (!e.id || e.id === t.id),
  Un = (e) => {
    J.has(e) ? zn(mt, e, null) : e.parentNode?.removeChild(e);
  },
  zn = (e, t, r) => {
    if ("moveBefore" in e) {
      e.moveBefore(t, r);
      return;
    }
    e.insertBefore(t, r);
  },
  Xr = se("preserve-attr"),
  Wt = (e, t) => {
    let r = t.nodeType;
    if (r === 1) {
      let o = e,
        n = t,
        i = o.hasAttribute("data-scope-children");
      if (o.hasAttribute(Be) && n.hasAttribute(Be)) return e;
      let s = (t.getAttribute(Xr) ?? "").split(" "),
        a = (l, u, d) => {
          let p = u.hasAttribute(d);
          return l.hasAttribute(d) !== p && !s.includes(d)
            ? ((l[d] = p), !0)
            : !1;
        },
        c = !1;
      if (
        o instanceof HTMLInputElement &&
        n instanceof HTMLInputElement &&
        n.type !== "file"
      ) {
        let l = n.getAttribute("value");
        o.getAttribute("value") !== l &&
          !s.includes("value") &&
          ((o.value = l ?? ""), (c = !0)),
          (c = a(o, n, "checked") || c),
          a(o, n, "disabled");
      } else if (
        o instanceof HTMLTextAreaElement &&
        n instanceof HTMLTextAreaElement
      ) {
        let l = n.value;
        o.defaultValue !== l && ((o.value = l), (c = !0));
      } else
        o instanceof HTMLOptionElement &&
          n instanceof HTMLOptionElement &&
          (c = a(o, n, "selected") || c);
      for (let { name: l, value: u } of n.attributes)
        o.getAttribute(l) !== u && !s.includes(l) && o.setAttribute(l, u);
      for (let { name: l } of Array.from(o.attributes))
        !n.hasAttribute(l) && !s.includes(l) && o.removeAttribute(l);
      c &&
        (o instanceof HTMLOptionElement
          ? o.closest("select")
          : o
        )?.dispatchEvent(new Event(Me, { bubbles: !0 })),
        i &&
          !o.hasAttribute("data-scope-children") &&
          o.setAttribute("data-scope-children", ""),
        o instanceof HTMLTemplateElement && n instanceof HTMLTemplateElement
          ? (o.innerHTML = n.innerHTML)
          : o.isEqualNode(n) || Jn(o, n),
        i && o.dispatchEvent(new CustomEvent(ve, { bubbles: !1 }));
    }
    return (
      (r === 8 || r === 3) &&
        e.nodeValue !== t.nodeValue &&
        (e.nodeValue = t.nodeValue),
      e
    );
  },
  Kn = (e, t) => {
    for (let r of t)
      if (Ae.has(r.id)) {
        let o = r;
        for (; o && o !== e; ) {
          let n = J.get(o);
          n || ((n = new Set()), J.set(o, n)),
            n.add(r.id),
            (o = o.parentElement);
        }
      }
  };
He({
  name: "datastar-patch-signals",
  apply({ error: e }, { signals: t, onlyIfMissing: r }) {
    if (typeof t != "string") throw e("PatchSignalsExpectedSignals");
    let o = typeof r == "string" && r.trim() === "true";
    U(le(t), { ifMissing: o });
  },
});
L({
  name: "clipboard",
  apply: ({ error: e }, t, r = !1) => {
    if (!navigator.clipboard) throw e("ClipboardNotAvailable");
    navigator.clipboard.writeText(r ? atob(t) : t);
  },
});
L({
  name: "fit",
  apply: (e, t, r, o, n, i, s = !1, a = !1) => {
    let c = pt(r, o, t, s),
      l = dt(n, i, c, s);
    return a && (l = Math.round(l)), l;
  },
});
L({
  name: "intl",
  apply({ error: e }, t, r, o, n) {
    let i = n || navigator.language || "en-US";
    switch (t) {
      case "datetime": {
        let s = r instanceof Date ? r : new Date(r);
        if (Number.isNaN(s.getTime())) throw e("IntlInvalidDate", { value: r });
        return new Intl.DateTimeFormat(i, o).format(s);
      }
      case "number":
        return new Intl.NumberFormat(i, o).format(
          typeof r == "number" ? r : parseFloat(r),
        );
      case "pluralRules":
        return new Intl.PluralRules(i, o).select(Number(r));
      case "relativeTime": {
        let s = o.unit?.[0] ?? "day";
        return new Intl.RelativeTimeFormat(i, o).format(Number(r), s);
      }
      case "list":
        return new Intl.ListFormat(i, o).format(
          Array.isArray(r) ? r.map(String) : [String(r)],
        );
      case "displayNames": {
        let s = o.type || "language",
          a = { ...o, type: s };
        return new Intl.DisplayNames(i, a).of(String(r));
      }
      default:
        throw e("IntlTypeNotSupported", { type: t });
    }
  },
});
A({
  name: "animate",
  requirement: { value: "must" },
  returnsValue: !0,
  apply({ el: e, key: t, mods: r, rx: o }) {
    let n = 1e3,
      i = r.get("duration");
    i && (n = Q(i));
    let s = ut(r.get("ease"), "linear"),
      a = ye[s];
    if (!a)
      throw new Error(
        `Invalid easing function: ${s}. Available easing functions: ${Object.keys(ye).join(", ")}`,
      );
    let c = 0,
      l = r.get("delay");
    l && (c = Q(l));
    let u = -1,
      d = () => {
        u !== -1 && (cancelAnimationFrame(u), (u = -1));
      },
      p = (b, g) => {
        let m = Zn(g),
          w = e.getAttribute(b) || `0${m.suffix}`;
        if (w === g) {
          e.setAttribute(b, g);
          return;
        }
        let E = Zn(w);
        if (E.suffix !== m.suffix)
          throw new Error(
            `Cannot animate attribute "${b}" from "${w}" to "${g}" because they have different suffixes: "${E.suffix}" and "${m.suffix}".`,
          );
        d();
        let x = 0,
          v = 0,
          f = () => {
            let h = performance.now(),
              S = h >= v,
              R = 0;
            if (S) {
              R = m.value;
              let k = r.has("loop"),
                T = r.has("pingpong");
              if (k || T) {
                if (((x = h), (v = h + n), T)) {
                  let y = E.value;
                  (E.value = m.value), (m.value = y);
                }
                (R = E.value), (S = !1);
              }
            } else {
              let k = pt(x, v, h),
                T = a(k);
              R = dt(E.value, m.value, T);
            }
            e.setAttribute(b, `${R}${m.suffix}`),
              S ? d() : (u = requestAnimationFrame(f));
          };
        setTimeout(() => {
          (x = performance.now()), (v = x + n), (u = requestAnimationFrame(f));
        }, c);
      };
    return t
      ? M(() => {
          let b = o();
          p(t, `${b}`);
        })
      : M(() => {
          let b = o();
          for (let [g, m] of Object.entries(b)) p(g, m);
        });
  },
});
var Zn = (e) => {
    let t = e.match(/^(-?\d*\.?\d+)([a-z%]*)$/i);
    if (t) return { value: Number.parseFloat(t[1]), suffix: t[2] || "" };
    throw new Error(`Invalid suffixed value: ${e}`);
  },
  { sqrt: gt, sin: We, cos: Qn, PI: we } = Math,
  ye = {
    linear: (e) => e,
    quadratic: (e) => e * (-(e * e) * e + 4 * e * e - 6 * e + 4),
    cubic: (e) => e * (4 * e * e - 9 * e + 6),
    elastic: (e) =>
      e * (33 * e * e * e * e - 106 * e * e * e + 126 * e * e - 67 * e + 15),
    inquad: (e) => e * e,
    outquad: (e) => e * (2 - e),
    inoutquad: (e) => (e < 0.5 ? 2 * e * e : -1 + (4 - 2 * e) * e),
    incubic: (e) => e * e * e,
    outcubic: (e) => --e * e * e + 1,
    inoutcubic: (e) =>
      e < 0.5 ? 4 * e * e * e : (e - 1) * (2 * e - 2) * (2 * e - 2) + 1,
    inquart: (e) => e * e * e * e,
    outquart: (e) => 1 - --e * e * e * e,
    inoutquart: (e) => (e < 0.5 ? 8 * e * e * e * e : 1 - 8 * --e * e * e * e),
    inquint: (e) => e * e * e * e * e,
    outquint: (e) => 1 + --e * e * e * e * e,
    inoutquint: (e) =>
      e < 0.5 ? 16 * e * e * e * e * e : 1 + 16 * --e * e * e * e * e,
    insine: (e) => -Qn(e * (we / 2)) + 1,
    outsine: (e) => We(e * (we / 2)),
    inoutsine: (e) => -(Qn(we * e) - 1) / 2,
    inexpo: (e) => 2 ** (10 * (e - 1)),
    outexpo: (e) => -(2 ** (-10 * e)) + 1,
    inoutexpo: (e) => (
      (e /= 0.5),
      e < 1 ? 2 ** (10 * (e - 1)) / 2 : (e--, (-(2 ** (-10 * e)) + 2) / 2)
    ),
    incirc: (e) => -gt(1 - e * e) + 1,
    outcirc: (e) => gt(1 - (e = e - 1) * e),
    inoutcirc: (e) => (
      (e /= 0.5),
      e < 1 ? -(gt(1 - e * e) - 1) / 2 : ((e -= 2), (gt(1 - e * e) + 1) / 2)
    ),
    inelastic: (e) => {
      let t = (2 * we) / 3;
      return e === 0
        ? 0
        : e === 1
          ? 1
          : -(2 ** (10 * (e - 1))) * We((e - 1.1) * t);
    },
    outelastic: (e) => {
      let t = (2 * we) / 3;
      return e === 0 ? 0 : e === 1 ? 1 : 2 ** (-10 * e) * We((e - 0.1) * t) + 1;
    },
    inoutelastic: (e) => {
      let t = (2 * we) / 4.5;
      return e === 0
        ? 0
        : e === 1
          ? 1
          : e < 0.5
            ? (-(2 ** (20 * e - 10)) * We((20 * e - 11.125) * t)) / 2
            : (2 ** (-20 * e + 10) * We((20 * e - 11.125) * t)) / 2 + 1;
    },
    inback: (e) => e * e * ((1.70158 + 1) * e - 1.70158),
    outback: (e) => (e = e - 1) * e * ((1.70158 + 1) * e + 1.70158) + 1,
    inoutback: (e) => {
      let t = 2.5949095;
      return (
        (e /= 0.5),
        e < 1
          ? (e * e * ((t + 1) * e - t)) / 2
          : ((e -= 2), (e * e * ((t + 1) * e + t) + 2) / 2)
      );
    },
    inbounce: (e) => 1 - ye.outBounce(1 - e),
    outbounce: (e) =>
      e < 1 / 2.75
        ? 7.5625 * e * e
        : e < 2 / 2.75
          ? ((e -= 1.5 / 2.75), 7.5625 * e * e + 0.75)
          : e < 2.5 / 2.75
            ? ((e -= 2.25 / 2.75), 7.5625 * e * e + 0.9375)
            : ((e -= 2.625 / 2.75), 7.5625 * e * e + 0.984375),
    inoutbounce: (e) =>
      e < 0.5 ? ye.inBounce(e * 2) / 2 : ye.outBounce(e * 2 - 1) / 2 + 0.5,
    ingolden: (e) => e ** 1.834057,
    outgolden: (e) => 1 - (1 - e) ** 1.834057,
    inoutgolden: (e) =>
      e < 0.5 ? ye.inGolden(e * 2) / 2 : ye.outGolden(e * 2 - 1) / 2 + 0.5,
  };
A({
  name: "custom-validity",
  requirement: { key: "denied", value: "must" },
  returnsValue: !0,
  apply({ el: e, rx: t, error: r }) {
    if (
      !(
        e instanceof HTMLInputElement ||
        e instanceof HTMLSelectElement ||
        e instanceof HTMLTextAreaElement
      )
    )
      throw r("CustomValidityInvalidElement", { element: { tag: e.tagName } });
    return M(() => {
      let o = t();
      if (typeof o != "string")
        throw r("CustomValidityInvalidExpression", {
          expression: { value: o },
        });
      e.setCustomValidity(o);
    });
  },
});
A({
  name: "match-media",
  requirement: { key: "must", value: "must" },
  apply({ key: e, mods: t, value: r }) {
    let o = _(e, t),
      n = r?.trim() ?? "";
    /^(["'`]).*\1$/.test(n) && (n = n.slice(1, -1).trim()),
      /[()]/.test(n) || (n = `(${n})`);
    let i;
    try {
      i = window.matchMedia(n);
    } catch {}
    let s = (a) => N([[o, a.matches]]);
    return (
      N([[o, i?.matches ?? !1]]),
      i?.addEventListener("change", s),
      () => {
        i?.removeEventListener("change", s), N([[o, null]]);
      }
    );
  },
});
A({
  name: "on-raf",
  requirement: { key: "denied", value: "must" },
  apply({ mods: e, rx: t }) {
    let r = () => {
      F(), t(), H();
    };
    (r = G(r, e)), (r = te(r, e));
    let o,
      n = () => {
        r(), (o = requestAnimationFrame(n));
      };
    return (
      (o = requestAnimationFrame(n)),
      () => {
        o && cancelAnimationFrame(o);
      }
    );
  },
});
A({
  name: "on-resize",
  requirement: { key: "denied", value: "must" },
  apply({ el: e, mods: t, rx: r }) {
    let o = () => {
      F(), r(), H();
    };
    (o = G(o, t)), (o = te(o, t));
    let n = new ResizeObserver(() => {
      o();
    });
    return (
      n.observe(e),
      () => {
        n && (n.disconnect(), (n = null));
      }
    );
  },
});
A({
  name: "persist",
  returnsValue: !0,
  apply({ key: e, mods: t, rx: r }) {
    let o = e || "datastar",
      n = t.has("session") ? sessionStorage : localStorage,
      i = n[o];
    if (i)
      try {
        let s = JSON.parse(i);
        U(s);
      } catch (s) {
        console.error("Failed to parse persisted data:", s);
      }
    return M(() => {
      let s = B(r?.()),
        a = JSON.stringify(s);
      n[o] = a;
    });
  },
});
A({
  name: "query-string",
  requirement: { key: "denied" },
  returnsValue: !0,
  apply({ mods: e, rx: t }) {
    let r = e.has("history"),
      o = !1,
      n = () => new URLSearchParams(window.location.search),
      i = () => B(t?.()),
      s = () => {
        let l = n(),
          u = i(),
          d = [],
          p = (g, m = "") => {
            let w = [];
            for (let E in g) {
              let x = m ? `${m}.${E}` : E,
                v = g[E];
              v && typeof v == "object" && !(v instanceof HTMLElement)
                ? w.push(...p(v, x))
                : w.push(x);
            }
            return w;
          },
          b = p(u);
        for (let g of b) {
          let m = l.get(g);
          if (m !== null) {
            let w = m;
            m === "true"
              ? (w = !0)
              : m === "false"
                ? (w = !1)
                : !Number.isNaN(+m) && m !== "" && (w = +m),
              d.push([g, w]);
          }
        }
        Qe(d) || N(d);
      };
    s();
    let a = () => {
      (o = !0), s(), (o = !1);
    };
    r && window.addEventListener("popstate", a);
    let c = M(() => {
      if (o) return;
      let l = new URLSearchParams(),
        u = n().toString(),
        d = (b, g) => {
          for (let m in b) {
            let w = b[m],
              E = g.length ? `${g}.${m}` : m;
            w instanceof HTMLElement ||
              (typeof w == "object"
                ? d(w, E)
                : (!e.has("filter") || w) && l.set(E, `${w}`));
          }
        };
      d(i(), "");
      let p = l.toString();
      if (p !== u) {
        let b = p ? `?${p}` : window.location.pathname;
        r
          ? window.history.pushState({}, "", b)
          : window.history.replaceState({}, "", b);
      }
    });
    return () => {
      r && window.removeEventListener("popstate", a), c();
    };
  },
});
A({
  name: "replace-url",
  requirement: { key: "denied", value: "must" },
  returnsValue: !0,
  apply({ rx: e }) {
    return M(() => {
      let t = e(),
        r = window.location.href,
        o = new URL(t, r).toString();
      window.history.replaceState({}, "", o);
    });
  },
});
var Gt = "smooth",
  Yn = "instant",
  Xn = "auto",
  eo = "hstart",
  to = "hcenter",
  no = "hend",
  ro = "hnearest",
  oo = "vstart",
  so = "vcenter",
  io = "vend",
  ao = "vnearest",
  ht = "center",
  er = "start",
  tr = "end",
  nr = "nearest",
  co = "focus";
A({
  name: "scroll-into-view",
  requirement: "denied",
  apply({ el: e, mods: t }) {
    let r = { behavior: Gt, block: ht, inline: ht };
    t.has(Gt) && (r.behavior = Gt),
      t.has(Yn) && (r.behavior = Yn),
      t.has(Xn) && (r.behavior = Xn),
      t.has(eo) && (r.inline = er),
      t.has(to) && (r.inline = ht),
      t.has(no) && (r.inline = tr),
      t.has(ro) && (r.inline = nr),
      t.has(oo) && (r.block = er),
      t.has(so) && (r.block = ht),
      t.has(io) && (r.block = tr),
      t.has(ao) && (r.block = nr),
      e.tabIndex || e.setAttribute("tabindex", "0"),
      e.scrollIntoView(r),
      t.has(co) && e.focus();
  },
});
A({
  name: "view-transition",
  requirement: { key: "denied", value: "must" },
  returnsValue: !0,
  apply({ el: e, rx: t }) {
    if (!qe) {
      console.warn("Browser does not support view transitions");
      return;
    }
    return M(() => {
      let r = t();
      r && (e.style.viewTransitionName = r);
    });
  },
});
var ue = (e) =>
    "defaultFactory" in e && typeof e.defaultFactory == "function"
      ? e.defaultFactory()
      : e.decode(void 0),
  Ee = (e, t) => {
    try {
      return e.decode(t);
    } catch (r) {
      return (
        console.warn("Rocket codec decode failed", { value: t, error: r }),
        ue(e)
      );
    }
  },
  yt = (e) => {
    if (e == null || typeof e != "object") return e;
    try {
      return structuredClone(e);
    } catch {
      return e;
    }
  },
  lo = (e) => (typeof e == "function" ? () => yt(e()) : () => yt(e)),
  z = (e, t, r) => ({
    default(o) {
      return e(lo(o), r);
    },
    docs(o) {
      return e(t, { ...(r ?? { type: "custom" }), docs: o });
    },
    defaultFactory: t,
    manifestMeta: r,
  }),
  rr = ({ decode: e, encode: t }, r, o = { type: "custom" }) => ({
    decode: e,
    encode: t,
    ...z((n, i) => rr({ decode: e, encode: t }, n, i ?? o), r, o),
  }),
  or = (e) => rr(e),
  uo = (e, t, r, o, n, i = !0, s = !1) => {
    if (r === t) return s ? Math.round(o) : o;
    let a = (e - t) / (r - t),
      c = i ? Math.max(0, Math.min(1, a)) : a,
      l = o + (n - o) * c;
    return s ? Math.round(l) : l;
  },
  ie = (e) => {
    if (typeof e == "string")
      try {
        return JSON.parse(e);
      } catch {
        return;
      }
    return e;
  },
  Jt = (
    e = (n) => String(n ?? ""),
    t = (n) => String(n ?? ""),
    r,
    o = { type: "string" },
  ) => {
    let n = (s) =>
        Jt(
          (a) => s(e(a)),
          (a) => s(t(a)),
          r,
          o,
        ),
      i = {
        decode: e,
        encode: t,
        ...z(Jt.bind(null, e, t), r, o),
        prefix(s) {
          return n((a) => (a.startsWith(s) ? a : s + a));
        },
        suffix(s) {
          return n((a) => (a.endsWith(s) ? a : a + s));
        },
        maxLength(s) {
          return n((a) => a.slice(0, s));
        },
      };
    return (
      Object.defineProperties(i, {
        trim: { get: () => n((s) => s.trim()) },
        upper: { get: () => n((s) => s.toUpperCase()) },
        lower: { get: () => n((s) => s.toLowerCase()) },
        kebab: {
          get: () =>
            n((s) =>
              s
                .trim()
                .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
                .replace(/[\s_]+/g, "-")
                .toLowerCase(),
            ),
        },
        camel: {
          get: () =>
            n((s) =>
              s
                .trim()
                .replace(/[-_\s]+(.)?/g, (a, c) => (c ? c.toUpperCase() : ""))
                .replace(/^(.)/, (a, c) => c.toLowerCase()),
            ),
        },
        snake: {
          get: () =>
            n((s) =>
              s
                .trim()
                .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
                .replace(/[-\s]+/g, "_")
                .toLowerCase(),
            ),
        },
        pascal: {
          get: () =>
            n((s) =>
              s.trim().replace(/(^|[-_\s]+)(.)/g, (a, c, l) => l.toUpperCase()),
            ),
        },
        title: {
          get: () =>
            n((s) =>
              s.replace(
                /\w\S*/g,
                (a) => a.charAt(0).toUpperCase() + a.slice(1).toLowerCase(),
              ),
            ),
        },
      }),
      i
    );
  },
  zt = (
    e = (n) => {
      let i = Number(n);
      return Number.isFinite(i) ? i : 0;
    },
    t = (n) => `${n}`,
    r,
    o = { type: "number" },
  ) => {
    let n = (s) =>
        zt(
          (a) => s(e(a)),
          (a) => `${s(Number(a))}`,
          r,
          o,
        ),
      i = {
        decode: e,
        encode: t,
        ...z(zt.bind(null, e, t), r, o),
        min(s) {
          return n((a) => Math.max(a, s));
        },
        max(s) {
          return n((a) => Math.min(a, s));
        },
        clamp(s, a) {
          return n((c) => Math.max(s, Math.min(a, c)));
        },
        step(s, a = 0) {
          return n((c) => a + Math.round((c - a) / s) * s);
        },
        ceil(s = 0) {
          return n((a) => Math.ceil(a * 10 ** s) / 10 ** s);
        },
        floor(s = 0) {
          return n((a) => Math.floor(a * 10 ** s) / 10 ** s);
        },
        fit(s, a, c, l, u = !0, d = !1) {
          return n((p) => uo(p, s, a, c, l, u, d));
        },
      };
    return (
      Object.defineProperties(i, {
        round: { get: () => n((s) => Math.round(s)) },
      }),
      i
    );
  },
  sr = (
    e = (n) => n === !0 || n === "" || n === "true" || n === 1 || n === "1",
    t = (n) => (n ? "true" : "false"),
    r,
    o = { type: "boolean" },
  ) => ({ decode: e, encode: t, ...z(sr.bind(null, e, t), r, o) }),
  ir = (
    e = (n) => {
      if (n instanceof Date) return Number.isNaN(n.getTime()) ? new Date() : n;
      let i = new Date(n);
      return Number.isNaN(i.getTime()) ? new Date() : i;
    },
    t = (n) => {
      let i = n instanceof Date ? n : new Date(n);
      return Number.isNaN(i.getTime())
        ? new Date().toISOString()
        : i.toISOString();
    },
    r,
    o = { type: "date" },
  ) => ({ decode: e, encode: t, ...z((n, i) => ir(e, t, n, i ?? o), r, o) }),
  ar = (
    e = (n) => {
      let i = ie(n);
      return i == null ? {} : yt(i);
    },
    t = (n) => JSON.stringify(n),
    r,
    o = { type: "json" },
  ) => ({ decode: e, encode: t, ...z((n, i) => ar(e, t, n, i ?? o), r, o) }),
  cr = (
    e = (n) => {
      if (typeof n == "string")
        try {
          return le(n, { reviveFunctionStrings: !0 });
        } catch {
          return {};
        }
      return n == null ? {} : yt(n);
    },
    t = (n) => JSON.stringify(n),
    r,
    o = { type: "js" },
  ) => ({ decode: e, encode: t, ...z((n, i) => cr(e, t, n, i ?? o), r, o) }),
  lr = (
    e = (n) => {
      if (n instanceof Uint8Array) return n;
      let i = atob(String(n ?? "")),
        s = new Uint8Array(i.length);
      for (let a = 0; a < i.length; a += 1) s[a] = i.charCodeAt(a);
      return s;
    },
    t = (n) => btoa(Array.from(n, (i) => String.fromCharCode(i)).join("")),
    r,
    o = { type: "binary" },
  ) => ({ decode: e, encode: t, ...z((n, i) => lr(e, t, n, i ?? o), r, o) }),
  ur = (e, t, r = { type: "array" }) => ({
    decode(o) {
      let n = ie(o);
      return (Array.isArray(n) ? n : []).map((s) => Ee(e, s));
    },
    encode(o) {
      return JSON.stringify(o.map((n) => ie(e.encode(n)) ?? e.encode(n)));
    },
    ...z((o, n) => ur(e, o, n ?? r), t, r),
  }),
  fr = (e, t, r = { type: "tuple" }) => ({
    decode(o) {
      let n = ie(o),
        i = Array.isArray(n) ? n : [];
      return e.map((s, a) => (a < i.length ? Ee(s, i[a]) : ue(s)));
    },
    encode(o) {
      return JSON.stringify(
        o.map((n, i) => {
          let s = e[i].encode(n);
          return ie(s) ?? s;
        }),
      );
    },
    ...z((o, n) => fr(e, o, n ?? r), t, r),
  }),
  dr = (e, t, r = { type: "object" }) => ({
    decode(o) {
      let n = ie(o),
        i = n && typeof n == "object" && !Array.isArray(n) ? n : {},
        s = {};
      for (let [a, c] of Object.entries(e)) s[a] = a in i ? Ee(c, i[a]) : ue(c);
      return s;
    },
    encode(o) {
      let n = {};
      for (let [i, s] of Object.entries(e)) {
        let a = s.encode(o[i]);
        n[i] = ie(a) ?? a;
      }
      return JSON.stringify(n);
    },
    ...z((o, n) => dr(e, o, n ?? r), t, r),
  }),
  Zt = (e) => !!e && typeof e == "object" && "decode" in e && "encode" in e,
  fo = (e, t) => {
    if (t) return t;
    let r = e[0];
    return Zt(r) ? () => ue(r) : () => r;
  },
  pr = (e, t, r = { type: "oneOf", values: e }) => ({
    decode(o) {
      let n = !1,
        i = ie(o);
      for (let s of e) {
        if (Zt(s))
          try {
            let a = s.decode(o),
              c = s.encode(a),
              l = ie(c) ?? c;
            if (c === o || JSON.stringify(l) === JSON.stringify(i)) return a;
          } catch {
            n = !0;
          }
        if (s === o || s === i || `${s}` == `${o}`) return s;
      }
      return (
        n && console.warn("Rocket codec decode failed", { value: o }),
        t ? t() : e[0]
      );
    },
    encode(o) {
      for (let n of e) {
        if (Zt(n)) return n.encode(o);
        if (n === o) return `${o}`;
      }
      return `${o}`;
    },
    ...z((o, n) => pr(e, o, n ?? r), fo(e, t), r),
  });
function po(...e) {
  return pr(e);
}
var mr = (e) =>
    "manifestMeta" in e && e.manifestMeta ? e.manifestMeta : { type: "custom" },
  gr = {
    string: Jt(),
    number: zt(),
    bool: sr(),
    date: ir(),
    json: ar(),
    js: cr(),
    bin: lr(),
    array: (...e) => (e.length === 1 ? ur(e[0]) : fr(e)),
    object: dr,
    oneOf: po,
  };
var hr = new Set(["script", "style", "textarea", "title"]),
  Yt = (e, t) => {
    if (!(t == null || t === !1)) {
      if (t instanceof Node) {
        e.appendChild(t);
        return;
      }
      for (let r of t)
        if (!(r == null || r === !1)) {
          if (r instanceof Node) {
            e.appendChild(r);
            continue;
          }
          e.appendChild(document.createTextNode(br(r)));
        }
    }
  };
function Xt(e, t) {
  return ({ raw: r }, ...o) => {
    let n = {
        context: "data",
        tagName: "",
        closingTag: !1,
        captureTagName: !1,
        rawTextTag: "",
      },
      i = "";
    for (let l = 0; l < r.length; ++l) {
      let u = r[l];
      if (l > 0) {
        let d = o[l - 1];
        if (
          d instanceof Node ||
          (typeof d == "object" && d !== null && Symbol.iterator in d)
        ) {
          if (n.context !== "data")
            throw new Error("invalid template composition");
          i += `<!--::${l}-->`;
        } else if (
          d == null ||
          d instanceof Date ||
          typeof d == "string" ||
          typeof d == "number" ||
          typeof d == "boolean" ||
          typeof d == "bigint"
        ) {
          if (!(d === !1 && n.context === "data")) {
            let p = r[l - 1],
              b = p.match(/(\s+[^\s"'<>/=]+)="$/),
              g = p.match(/(\s+[^\s"'<>/=]+)='$/),
              m = b || g ? null : p.match(/(\s+[^\s"'<>/=]+)=$/),
              w =
                (b && u.startsWith('"')) ||
                (g && u.startsWith("'")) ||
                (m && (u === "" || /^[\s/>]/.test(u))),
              E = b
                ? "attrDouble"
                : g
                  ? "attrSingle"
                  : m
                    ? "attrUnquoted"
                    : n.context,
              x =
                w && (d == null || d === !1 || d === "" || d === !0)
                  ? ""
                  : (() => {
                      let v = br(d);
                      switch (E) {
                        case "attrDouble":
                          return v.replace(/["&]/g, Ue);
                        case "attrSingle":
                          return v.replace(/['&]/g, Ue);
                        case "attrUnquoted":
                          return v === "" ? "''" : v.replace(/[\s>&]/g, Ue);
                        case "rawtext":
                          return v.replace(/[<]/g, Ue);
                        default:
                          return v.replace(/[<&]/g, Ue);
                      }
                    })();
            w && (d == null || d === !1 || d === "")
              ? ((i = i.slice(
                  0,
                  -(b?.[0].length ?? g?.[0].length ?? m?.[0].length ?? 0),
                )),
                (b || g) && (u = u.slice(1)))
              : w && d === !0
                ? ((i =
                    i.slice(
                      0,
                      -(b?.[0].length ?? g?.[0].length ?? m?.[0].length ?? 0),
                    ) + (b?.[1] ?? g?.[1] ?? m?.[1] ?? "")),
                  (b || g) && (u = u.slice(1)))
                : (i += x);
          }
        } else throw new Error("invalid template value");
      }
      for (let d = 0; d < u.length; d += 1) {
        let p = u[d];
        switch (n.context) {
          case "data":
            if (
              n.rawTextTag &&
              p === "<" &&
              u.slice(d + 1, d + 2 + n.rawTextTag.length).toLowerCase() ===
                `/${n.rawTextTag}` &&
              (!u[d + 2 + n.rawTextTag.length] ||
                /[\s/>]/.test(u[d + 2 + n.rawTextTag.length] ?? ""))
            ) {
              (n.context = "tag"),
                (n.tagName = ""),
                (n.closingTag = !0),
                (n.captureTagName = !1);
              continue;
            }
            p === "<" &&
              ((n.context = "tag"),
              (n.tagName = ""),
              (n.closingTag = u[d + 1] === "/"),
              (n.captureTagName = !n.closingTag));
            break;
          case "rawtext":
            p === "<" &&
              u.slice(d + 1, d + 2 + n.rawTextTag.length).toLowerCase() ===
                `/${n.rawTextTag}` &&
              (!u[d + 2 + n.rawTextTag.length] ||
                /[\s/>]/.test(u[d + 2 + n.rawTextTag.length] ?? "")) &&
              ((n.context = "tag"),
              (n.tagName = ""),
              (n.closingTag = !0),
              (n.captureTagName = !1));
            break;
          case "tag":
            if (!n.tagName) {
              if (p === "!" || p === "?") {
                (n.context = "data"),
                  (n.closingTag = !1),
                  (n.captureTagName = !1);
                continue;
              }
              if ((n.closingTag && p === "/") || /\s/.test(p)) continue;
              if (p === ">") {
                (n.context = n.rawTextTag ? "rawtext" : "data"),
                  (n.closingTag = !1),
                  (n.captureTagName = !1);
                continue;
              }
              n.tagName = p.toLowerCase();
              continue;
            }
            if (p && !/[\s/>]/.test(p)) {
              n.captureTagName && (n.tagName += p.toLowerCase());
              continue;
            }
            if (p === ">") {
              n.closingTag
                ? (n.tagName === n.rawTextTag && (n.rawTextTag = ""),
                  (n.context = "data"))
                : hr.has(n.tagName)
                  ? ((n.rawTextTag = n.tagName), (n.context = "rawtext"))
                  : (n.context = "data"),
                (n.closingTag = !1),
                (n.captureTagName = !1);
              continue;
            }
            if (p === '"') {
              n.context = "attrDouble";
              continue;
            }
            if (p === "'") {
              n.context = "attrSingle";
              continue;
            }
            if (p === "=") continue;
            !/\s/.test(p) && p !== "/" && (n.context = "attrUnquoted");
            break;
          case "attrDouble":
            p === '"' && (n.context = "tag");
            break;
          case "attrSingle":
            p === "'" && (n.context = "tag");
            break;
          case "attrUnquoted":
            /\s/.test(p)
              ? (n.context = "tag")
              : p === ">" &&
                (n.closingTag
                  ? (n.tagName === n.rawTextTag && (n.rawTextTag = ""),
                    (n.context = "data"))
                  : hr.has(n.tagName)
                    ? ((n.rawTextTag = n.tagName), (n.context = "rawtext"))
                    : (n.context = "data"),
                (n.closingTag = !1),
                (n.captureTagName = !1));
            break;
        }
      }
      i += u;
    }
    let s = e(i),
      a = document.createTreeWalker(s, NodeFilter.SHOW_COMMENT),
      c = [];
    for (; a.nextNode(); ) {
      let l = a.currentNode;
      if (!/^::/.test(l.data)) continue;
      let u = l.parentNode;
      if (!u) continue;
      let d = document.createDocumentFragment();
      Yt(d, o[+l.data.slice(2) - 1]), u.insertBefore(d, l), c.push(l);
    }
    for (let l of c) l.remove();
    return t(s);
  };
}
var mo = new Set(["bind", "computed", "indicator", "ref", "signals"]),
  go = new Set(["bind", "indicator", "ref"]),
  Qt = /__root\b/g,
  en = "dispatchRocket",
  xe = "data-rocket-ref",
  ho = (e) => {
    let t = new Map();
    for (let r of e.split("__").slice(1)) {
      let [o, ...n] = r.split(".");
      t.set(o, new Set(n));
    }
    return t;
  },
  yr = (e) => {
    let t = e.split("__", 1)[0];
    return _(t, ho(e));
  },
  bt = (e, { signalPathBase: t, localSignals: r = {} }) => {
    let o = e.replace(
      /\$\$([a-zA-Z_\d]\w*(?:[.-]\w+)*)/g,
      (n, i) => `$.${t}.${i}`,
    );
    for (let [n, i] of Object.entries(r)) {
      let s = n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      o = o.replace(
        new RegExp(
          `("(?:\\\\.|[^"\\\\])*"|'(?:\\\\.|[^'\\\\])*'|\`(?:\\\\.|[^\`\\\\$]|\\$(?!\\{))*\`)|\\$\\{([^{}]*)\\}|(?<![\\w$.])${s}(?![\\w$]|\\s*:)`,
          "g",
        ),
        (a, c, l) =>
          c
            ? a
            : l !== void 0
              ? "${" +
                l.replace(
                  new RegExp(`(?<![\\w$.])${s}(?![\\w$]|\\s*:)`, "g"),
                  `$.${i}`,
                ) +
                "}"
              : `$.${i}`,
      );
    }
    return o.replace(
      /@([A-Za-z_$][\w$]*)\(/g,
      (n, i) => `@${en}(${JSON.stringify(i)},`,
    );
  },
  be = (e, t) => {
    if (!t) return;
    let { signalPathBase: r, localSignals: o = {} } = t,
      n =
        e instanceof Element
          ? [e, ...e.querySelectorAll("*")]
          : [...e.children, ...e.querySelectorAll("*")];
    for (let i of n)
      for (let s of [...i.attributes]) {
        if (s.name === "data-signals") {
          if (i.hasAttribute(`data-signals:${r}`)) {
            i.removeAttribute(s.name);
            continue;
          }
          i.removeAttribute(s.name),
            i.setAttribute(`data-signals:${r}`, s.value);
          continue;
        }
        if (s.name.startsWith("data-signals:")) {
          let b = s.name.slice(13);
          if (b === r || b.startsWith(`${r}.`)) continue;
          i.removeAttribute(s.name),
            i.setAttribute(`data-signals:${r}.${b}`, s.value);
          continue;
        }
        if (!s.name.startsWith("data-")) continue;
        let a = s.name.slice(5),
          c = a.indexOf(":"),
          d = (c === -1 ? a : a.slice(0, c)).split("__", 1)[0].split(":", 1)[0];
        if (d === "ref") {
          let b = s.name.indexOf(":"),
            g = yr(b !== -1 ? s.name.slice(b + 1) : s.value);
          i.removeAttribute(s.name), g && i.setAttribute(xe, g);
          continue;
        }
        if (mo.has(d)) {
          let b = s.name.indexOf(":");
          if (b !== -1) {
            let g = s.name.slice(b + 1),
              m = g.split("__", 1)[0];
            if (g.includes("__root")) {
              let f = bt(s.value, t),
                h = `${s.name.slice(0, b + 1).replace(Qt, "")}${g.replace(Qt, "")}`;
              h !== s.name
                ? (i.removeAttribute(s.name), i.setAttribute(h, f))
                : f !== s.value && i.setAttribute(s.name, f);
              continue;
            }
            let E = o[m];
            if (E) {
              let f = bt(s.value, t);
              s.name !== `${s.name.slice(0, b + 1)}${E}`
                ? (i.removeAttribute(s.name),
                  i.setAttribute(
                    `${s.name.slice(0, b + 1)}${E}${g.slice(m.length)}`,
                    f,
                  ))
                : f !== s.value && i.setAttribute(s.name, f);
              continue;
            }
            let x = m.startsWith(`${r}.`) ? g : g.replace(m, `${r}.${m}`),
              v = bt(s.value, t);
            x !== g
              ? (i.removeAttribute(s.name),
                i.setAttribute(`${s.name.slice(0, b + 1)}${x}`, v))
              : v !== s.value && i.setAttribute(s.name, v);
            continue;
          }
          if (go.has(d)) {
            if (a.includes("__root")) {
              let w = s.name.replace(Qt, "");
              w !== s.name &&
                (i.removeAttribute(s.name), i.setAttribute(w, s.value));
              continue;
            }
            if (d === "ref" && s.value) {
              if (s.value === r || s.value.startsWith(`${r}.`)) continue;
              let w = o[s.value];
              if (w) {
                i.setAttribute(s.name, w);
                continue;
              }
              i.setAttribute(s.name, `${r}.${s.value}`);
              continue;
            }
            let m = o[s.value];
            if (m) {
              i.setAttribute(s.name, m);
              continue;
            }
            s.value.startsWith("$$") &&
              i.setAttribute(s.name, `${r}.${s.value.slice(2)}`);
            continue;
          }
        }
        let p = bt(s.value, t);
        p !== s.value && i.setAttribute(s.name, p);
      }
  };
function Ue(e) {
  return `&#${e.charCodeAt(0).toString()};`;
}
function br(e) {
  if (e instanceof Date) return e.toISOString();
  switch (typeof e) {
    case "string":
      return e;
    case "number":
    case "boolean":
    case "bigint":
      return `${e}`;
    case "object":
      return JSON.stringify(e);
    default:
      throw new Error("invalid template value");
  }
}
var tn = new WeakMap(),
  Ke = (e) => {
    if (e.hasAttribute("data-if")) return "if";
    if (e.hasAttribute("data-else-if")) return "else-if";
    if (e.hasAttribute("data-else")) return "else";
  },
  Ge = (e, t) => {
    throw new Error(`${e}
Context: ${JSON.stringify({ id: t.id, tag: t.tagName })}`);
  },
  yo = (e, t, r) => {
    let o = [
        {
          template: e,
          kind: "if",
          rx: Re(e.getAttribute("data-if") || "", { returnsValue: !0 }),
        },
      ],
      n = e,
      i = !1;
    for (
      let p = e.nextElementSibling;
      p instanceof HTMLTemplateElement;
      p = p.nextElementSibling
    ) {
      let b = Ke(p);
      if (!b || b === "if") break;
      if (b === "else") {
        i && Ge("RocketConditionalDuplicateElse", p),
          o.push({ template: p, kind: b }),
          (n = p),
          (i = !0);
        continue;
      }
      i && Ge("RocketConditionalElseIfAfterElse", p),
        o.push({
          template: p,
          kind: b,
          rx: Re(p.getAttribute("data-else-if") || "", { returnsValue: !0 }),
        }),
        (n = p);
    }
    let s = document.createComment("rocket-if:start"),
      a = document.createComment("rocket-if:end");
    n.parentNode.insertBefore(s, n.nextSibling),
      s.parentNode.insertBefore(a, s.nextSibling);
    let c = -1,
      l = [],
      u = () => {
        for (let p of l) p();
        l = [];
        for (let p = s.nextSibling; p && p !== a; ) {
          let b = p.nextSibling;
          p.remove(), (p = b);
        }
      },
      d = M(() => {
        let p = o.findIndex((m) =>
          m.kind === "else" ? !0 : !!m.rx?.(m.template),
        );
        if (p === c || (u(), (c = p), p === -1)) return;
        let b = document.importNode(o[p].template.content, !0);
        be(b, t);
        let g = [...b.childNodes];
        a.parentNode.insertBefore(b, a);
        for (let m of g) m instanceof Element && l.push(...r(m, t));
      });
    return () => {
      d(), u(), s.remove(), a.remove(), tn.delete(e);
    };
  },
  Tr = (e, t, r) => {
    let o = [],
      n = [...e.querySelectorAll("template")];
    e instanceof HTMLTemplateElement && n.unshift(e);
    for (let i of n) {
      if (!Ke(i)) continue;
      if (
        (Number(i.hasAttribute("data-if")) +
          Number(i.hasAttribute("data-else-if")) +
          Number(i.hasAttribute("data-else")) !==
          1 && Ge("RocketConditionalTemplateHasMultipleConditions", i),
        Ke(i) !== "if")
      ) {
        let a = i.previousElementSibling;
        a instanceof HTMLTemplateElement ||
          Ge("RocketConditionalMissingPreviousBranch", i);
        let c = Ke(a);
        (!c || c === "else") && Ge("RocketConditionalInvalidPreviousBranch", i);
      }
      if (Ke(i) !== "if" || tn.has(i)) continue;
      let s = yo(i, t, r);
      tn.set(i, s), o.push(s);
    }
    return o;
  };
var nn = new WeakMap(),
  bo = 0,
  vr = (e, t, r) => {
    let o = [],
      n = [...e.querySelectorAll("template[data-for]")];
    e instanceof HTMLTemplateElement &&
      e.hasAttribute("data-for") &&
      n.unshift(e);
    for (let i of n) {
      if (nn.has(i)) continue;
      let s = i.getAttribute("data-for")?.trim() || "";
      if (!s)
        throw new Error(`RocketForMissingExpression
Context: ${JSON.stringify({ id: i.id, tag: i.tagName })}`);
      let a =
        s.match(
          /^(?<item>[A-Za-z_$][\w$]*)\s*,\s*(?<index>[A-Za-z_$][\w$]*)\s+in\s+(?<source>[\s\S]+)$/,
        ) ?? s.match(/^(?<item>[A-Za-z_$][\w$]*)\s+in\s+(?<source>[\s\S]+)$/);
      if (!a && /^[A-Za-z_$][\w$]*(?:\s*,|\s+in\b)/.test(s))
        throw new Error(`RocketForInvalidExpression
Context: ${JSON.stringify({ id: i.id, tag: i.tagName, value: s })}`);
      let { item: c = "item", source: l = s, index: u = "i" } = a?.groups ?? {},
        d = `c${bo++}`,
        p = Re(l.trim(), { returnsValue: !0 }),
        b = document.createComment("rocket-for:start"),
        g = document.createComment("rocket-for:end");
      i.parentNode.insertBefore(b, i.nextSibling),
        b.parentNode.insertBefore(g, b.nextSibling);
      let m = [],
        w = M(() => {
          let x = p(i),
            v =
              x == null
                ? []
                : Array.isArray(x)
                  ? x
                  : typeof x == "string"
                    ? [...x]
                    : typeof x == "object" && Symbol.iterator in x
                      ? [...x]
                      : (() => {
                          throw new Error(`RocketForExpectedIterable
Context: ${JSON.stringify({ id: i.id, tag: i.tagName, valueType: typeof x })}`);
                        })();
          for (let h = v.length; h < m.length; h += 1) {
            let S = m[h];
            if (S) {
              for (let R of S.cleanups) R();
              for (let R = S.start; R; ) {
                let k = R.nextSibling;
                if ((R.remove(), R === S.end)) break;
                R = k;
              }
              V();
              try {
                N([[S.pathBase, null]]);
              } finally {
                q();
              }
            }
          }
          m.length = v.length;
          let f = g;
          for (let h = v.length - 1; h >= 0; h -= 1) {
            let S = `${t.signalPathBase}._for.${d}.row${h}`;
            V();
            try {
              N([
                [
                  `${S}.${c}`,
                  v[h] == null || typeof v[h] != "object"
                    ? v[h]
                    : (() => {
                        try {
                          return structuredClone(v[h]);
                        } catch {
                          return JSON.parse(JSON.stringify(v[h]));
                        }
                      })(),
                ],
                [`${S}.${u}`, h],
              ]);
            } finally {
              q();
            }
            let R = m[h];
            if (!R) {
              V();
              try {
                let k = document.importNode(i.content, !0),
                  T = {
                    ...(t.localSignals ?? {}),
                    [c]: `${S}.${c}`,
                    [u]: `${S}.${u}`,
                  };
                be(k, { ...t, localSignals: T });
                let y = document.createComment("rocket-for:row:start"),
                  C = document.createComment("rocket-for:row:end"),
                  O = [...k.childNodes];
                g.parentNode.insertBefore(y, f),
                  g.parentNode.insertBefore(C, f),
                  g.parentNode.insertBefore(k, C),
                  (R = { start: y, end: C, pathBase: S, cleanups: [] });
                for (let P of O)
                  P instanceof Element &&
                    R.cleanups.push(...r(P, { ...t, localSignals: T }));
              } finally {
                q();
              }
              m[h] = R;
            }
            f = R.start;
          }
        }),
        E = () => {
          w();
          for (let x of m)
            if (x) {
              for (let v of x.cleanups) v();
              V();
              try {
                N([[x.pathBase, null]]);
              } finally {
                q();
              }
              for (let v = x.start; v; ) {
                let f = v.nextSibling;
                if ((v.remove(), v === x.end)) break;
                v = f;
              }
            }
          (m.length = 0), b.remove(), g.remove(), nn.delete(i);
        };
      nn.set(i, E), o.push(E);
    }
    return o;
  };
var To = "data-rocket-host",
  Sr = (e, t, r) => ({
    el: e,
    evt: t,
    error: (o, n) => {
      let i = new Error(o);
      return (i.ctx = n), i;
    },
    cleanups: r,
  }),
  vt = "data-rocket-deferred-ignore",
  rn = (e) => {
    let t =
      e instanceof Element
        ? [e, ...e.querySelectorAll("*")]
        : [...e.querySelectorAll("*")];
    for (let r of t) {
      if (!(r instanceof HTMLElement)) continue;
      let o = r.tagName.toLowerCase();
      if (
        !o.includes("-") ||
        customElements.get(o) ||
        r.hasAttribute(vt) ||
        r.hasAttribute("data-ignore")
      )
        continue;
      let n = !1;
      for (let i of [r, ...r.querySelectorAll("*")]) {
        for (let s of i.attributes)
          if (s.name.startsWith("data-") && s.value.includes("$$")) {
            n = !0;
            break;
          }
        if (n) break;
      }
      n && (r.setAttribute("data-ignore", ""), r.setAttribute(vt, ""));
    }
  },
  Rr = () => {
    rn(document),
      new MutationObserver((e) => {
        for (let t of e) {
          if (t.type === "attributes" && t.target instanceof Element) {
            rn(t.target);
            continue;
          }
          for (let r of t.addedNodes) r instanceof Element && rn(r);
        }
      }).observe(document.documentElement, {
        subtree: !0,
        childList: !0,
        attributes: !0,
      });
  },
  Tt = !1,
  on = !1,
  Cr = !1,
  Te = new Map(),
  Ar = () => {
    if (
      (Cr ||
        ((Cr = !0),
        document.readyState === "loading"
          ? document.addEventListener("DOMContentLoaded", Rr, { once: !0 })
          : Rr()),
      !(Tt || on))
    ) {
      if (at()) {
        if (((Tt = !0), Te.size === 0)) return;
        for (let [e, t] of Te) Te.delete(e), St(e, t);
        return;
      }
      (on = !0),
        document.addEventListener(
          Ze,
          () => {
            if (((on = !1), !Tt && ((Tt = !0), Te.size !== 0)))
              for (let [e, t] of Te) Te.delete(e), St(e, t);
          },
          { once: !0 },
        );
    }
  };
Ar();
var sn = new Set(),
  an = new Map(),
  vo = 0,
  wr = (e) => ({
    tag: e.tag,
    props: e.props.map((t) => ({
      ...t,
      values: t.values ? [...t.values] : void 0,
      docs: t.docs ? { ...t.docs } : void 0,
    })),
    slots: e.slots.map((t) => ({ ...t })),
    events: e.events.map((t) => ({ ...t })),
  }),
  So = () => ({
    version: 1,
    generatedAt: new Date().toISOString(),
    components: [...an.values()]
      .map(wr)
      .sort((e, t) => e.tag.localeCompare(t.tag)),
  }),
  Er = async ({ endpoint: e, headers: t }) =>
    fetch(e, {
      method: "POST",
      headers: { "content-type": "application/json", ...t },
      body: JSON.stringify(So()),
    }),
  ke = new WeakMap();
L({
  name: en,
  apply({ el: e, evt: t, cleanups: r }, o, ...n) {
    let i = ke.get(e);
    if (i) return i.dispatchRocketAction(o, e, t, r, ...n);
  },
});
function St(e, t) {
  if (!e) return;
  if (!at()) {
    Te.set(e, t ?? {}), Ar();
    return;
  }
  let {
    manifest: r,
    mode: o,
    props: n,
    render: i,
    renderOnPropChange: s,
    setup: a,
    onFirstRender: c,
  } = t ?? {};
  if (sn.has(e) || customElements.get(e))
    return sn.add(e), customElements.get(e);
  sn.add(e);
  let l = n?.(gr) ?? {},
    u = Object.keys(l),
    d = new Map(u.map((v) => [ee(v), v])),
    p = {
      tag: e,
      props: u.map((v) => {
        let f = l[v],
          h = mr(f);
        return {
          name: v,
          attribute: ee(v),
          type: h.type,
          default: ue(f),
          required: !1,
          values: h.values ? [...h.values] : void 0,
          docs: h.docs ? { ...h.docs } : void 0,
        };
      }),
      slots: (r?.slots ?? []).map((v) => ({ ...v })),
      events: (r?.events ?? []).map((v) => ({ kind: "event", ...v })),
    },
    b = De(e),
    g = (v) => {
      let f = Z;
      for (let h of v.split(".")) {
        if (
          f == null ||
          (typeof f != "object" && typeof f != "function") ||
          !(h in f)
        )
          return;
        f = f[h];
      }
      return f;
    },
    m = (v) => {
      let f = Z;
      for (let h of v.split(".")) {
        if (
          f == null ||
          (typeof f != "object" && typeof f != "function") ||
          !(h in f)
        )
          return !1;
        f = f[h];
      }
      return !0;
    },
    w = (v, f) => {
      let h =
        v instanceof Element
          ? [v, ...v.querySelectorAll("*")]
          : [...v.children, ...v.querySelectorAll("*")];
      for (let S of h)
        for (let R of S.attributes) {
          if (!R.name.startsWith("data-")) continue;
          if (
            R.name === `data-signals:${f}` ||
            R.name.startsWith(`data-signals:${f}.`)
          )
            return !0;
          let k = R.name.indexOf(":");
          if (k !== -1) {
            let T = R.name.slice(k + 1).split("__", 1)[0];
            if (T === f || T.startsWith(`${f}.`)) return !0;
          }
          if (
            R.value === f ||
            R.value.startsWith(`${f}.`) ||
            R.value.includes(`$.${f}`)
          )
            return !0;
        }
      return !1;
    },
    E = (v, f) => [...vr(v, f, E), ...Tr(v, f, E)];
  class x extends HTMLElement {
    #m = "";
    #t = "";
    #r = Object.fromEntries(u.map((f) => [f, ue(l[f])]));
    #n = {};
    #i = {};
    #a = [];
    #g = new Map();
    #c = [];
    #o = [];
    #l = [];
    #h = new Map();
    #y = !1;
    #u = !1;
    #b = !1;
    #T = !1;
    #e;
    #p = !1;
    #v = !1;
    #f = !1;
    #S = new Map();
    #R;
    #s = (f) => {
      be(f, { signalPathBase: this.#t }), this.#f || (this.#f = w(f, this.#t));
    };
    #d = () => {
      let f = {},
        h = this.#e === this || !this.#e ? [this] : [this, this.#e];
      for (let S of h) {
        let R =
          S instanceof Element
            ? [
                ...(S.hasAttribute(xe) ? [S] : []),
                ...S.querySelectorAll(`[${xe}]`),
              ]
            : [...S.querySelectorAll(`[${xe}]`)];
        for (let k of R) {
          let T = k.getAttribute(xe);
          T && (f[T] = k);
        }
      }
      for (let S of Object.keys(this.#n)) delete this.#n[S];
      Object.assign(this.#n, f);
    };
    #x = () => {
      if (this.#e === this) {
        this.#s(this), this.#f && N([[this.#t, {}]], { ifMissing: !0 });
        for (let f of this.#o) f();
        (this.#o.length = 0),
          this.#o.push(...E(this, { signalPathBase: this.#t })),
          _e(this, !1),
          this.#d();
      } else {
        for (let f of this.children) this.#s(f);
        this.#f && N([[this.#t, {}]], { ifMissing: !0 });
        for (let f of this.#l) f();
        this.#l.length = 0;
        for (let f of this.children)
          this.#l.push(...E(f, { signalPathBase: this.#t })),
            (f instanceof HTMLElement || f instanceof SVGElement) && _e(f, !1);
        this.#d();
      }
    };
    #P = Xt(
      (f) => {
        let h = document.createElement("template");
        return (h.innerHTML = f), document.importNode(h.content, !0);
      },
      (f) => (this.#s(f), f),
    );
    #M = Xt(
      (f) => {
        let h = document.createElementNS("http://www.w3.org/2000/svg", "g");
        return (h.innerHTML = f), h;
      },
      (f) => {
        let h = document.createDocumentFragment();
        for (; f.firstChild; ) h.appendChild(f.firstChild);
        return this.#s(h), h;
      },
    );
    static get observedAttributes() {
      return [...d.keys()];
    }
    get rocketInstanceId() {
      return this.#m;
    }
    get rocketSignalPath() {
      return this.#t;
    }
    constructor() {
      super();
      let f = De(this.id.trim())
        .replace(/[^A-Za-z0-9_$]/g, "_")
        .replace(/_+/g, "_")
        .replace(/^_+|_+$/g, "");
      if (
        ((this.#m = f ? (/^[A-Za-z_$]/.test(f) ? f : `id_${f}`) : `id${++vo}`),
        (this.#t = `_rocket.${b}.${this.#m}`),
        this.addEventListener(ve, this.#x),
        this.childNodes.length)
      ) {
        if (o === "light") this.#s(this);
        else for (let h of this.children) this.#s(h);
        this.#v = !0;
      }
      for (let [h, S] of d) {
        let R = this.getAttribute(h);
        R !== null && (this.#S.set(h, R), (this.#r[S] = Ee(l[S], R)));
      }
      for (let h of u) {
        if (!Object.prototype.hasOwnProperty.call(this, h)) continue;
        let S = this[h];
        delete this[h], (this[h] = S);
      }
    }
    dispatchRocketAction(f, h, S, R, ...k) {
      let T = this.#g.get(f);
      if (T)
        return T(
          { host: this, props: this.#r, state: this.#i, el: h, evt: S },
          ...k,
        );
      for (let C = this; C; ) {
        let O = ke.get(C),
          P = C.getRootNode();
        if (O && O !== this) return O.dispatchRocketAction(f, h, S, R, ...k);
        C = C.parentElement ?? (P instanceof ShadowRoot ? P.host : null);
      }
      let y = Fe[f];
      if (y) return y(Sr(h ?? this, S, R), ...k);
      throw Error(`Undefined Rocket action: ${f}`);
    }
    #O() {
      !this.#u ||
        this.#b ||
        ((this.#b = !0),
        queueMicrotask(() => {
          (this.#b = !1), this.#u && this.#E({});
        }));
    }
    #C() {
      ke.set(this, this);
      for (let f of this.querySelectorAll("*")) ke.set(f, this);
      this.#e instanceof Element && ke.set(this.#e, this);
      for (let f of this.#e.querySelectorAll("*")) ke.set(f, this);
    }
    #k(f, h) {
      if (Object.is(this.#r[f], h)) return;
      this.#r[f] = h;
      let S = { [f]: h };
      for (let R of this.#a)
        (R.names === null || R.names.has(f)) && R.fn(this.#r, S);
      (typeof s == "function"
        ? s({ host: this, props: this.#r, changes: S })
        : s !== !1) && this.#O();
    }
    #A(f) {
      return this.#r[f];
    }
    #D(f, h) {
      if (o !== "light" && !this.isConnected) return;
      let S = ee(f);
      this.#T = !0;
      try {
        let R = l[f].encode(h);
        R == null ? this.removeAttribute(S) : this.setAttribute(S, R);
      } finally {
        this.#T = !1;
      }
    }
    #w(f, h) {
      this.#k(f, h), this.#D(f, h);
    }
    #N(f, h) {
      let S = Object.getOwnPropertyDescriptor(this, f),
        R = S && "value" in S ? S.value : void 0;
      S && delete this[f],
        Object.defineProperty(this, f, { configurable: !0, ...h }),
        S &&
          "value" in S &&
          R !== void 0 &&
          typeof h.set == "function" &&
          (this[f] = R);
    }
    #L(f, h, S) {
      this.#N(f, {
        get: h ? () => h(() => this.#A(f)) : () => this.#A(f),
        set: S ? (R) => S(R, (k) => this.#w(f, k)) : (R) => this.#w(f, R),
      });
    }
    #E(f, ...h) {
      if (!i) {
        this.#C();
        return;
      }
      if (this.#y) {
        for (let k of this.#o) k();
        this.#o.length = 0;
      }
      let S = i(
          { html: this.#P, svg: this.#M, props: this.#r, host: this, ...f },
          ...h,
        ),
        R;
      if (
        (S instanceof DocumentFragment
          ? (R = S)
          : ((R = document.createDocumentFragment()),
            S != null && !(S instanceof Node)
              ? Yt(R, S)
              : S != null && R.appendChild(document.createTextNode(`${S}`))),
        this.#e === this)
      ) {
        let k = [...R.querySelectorAll("slot")];
        if (k.length) {
          if (!this.#R) {
            let y = document.createDocumentFragment();
            y.append(...this.childNodes),
              be(y, { signalPathBase: this.#t }),
              (this.#R = [...y.childNodes]);
          }
          let T = new Set();
          for (let y of k) {
            let C = y.getAttribute("name"),
              O = this.#R.filter((P) => {
                if (T.has(P)) return !1;
                if (!(P instanceof Element)) return C === null || C === "";
                let D = P.getAttribute("slot");
                return C === null || C === ""
                  ? D === null || D === ""
                  : D === C;
              });
            if (O.length) {
              for (let P of O) T.add(P);
              y.replaceWith(...O);
              continue;
            }
            y.replaceWith(...y.childNodes);
          }
        }
      }
      Kt(this.#e, R, "inner"),
        this.#y && this.#o.push(...E(this.#e, { signalPathBase: this.#t })),
        this.#d(),
        this.#C();
    }
    connectedCallback() {
      if (this.#u) return;
      if (
        ((this.#u = !0),
        this.hasAttribute(vt)
          ? (this.removeAttribute(vt), (this.#p = !0))
          : this.hasAttribute("data-ignore") ||
            (this.setAttribute("data-ignore", ""), (this.#p = !0)),
        this.setAttribute("data-scope-children", ""),
        this.setAttribute(To, ""),
        this.#e ||
          (this.#e =
            o === "light"
              ? this
              : (this.shadowRoot ??
                this.attachShadow({
                  mode: o === "closed" ? "closed" : "open",
                }))),
        !this.#v)
      ) {
        if (this.#e === this) this.#s(this);
        else for (let T of this.children) this.#s(T);
        this.#v = !0;
      }
      let f = new Proxy(
          (T, y) => {
            let C = `${this.#t}.${T}`;
            return (
              N([[C, typeof y == "function" ? pe(y) : y]], { ifMissing: !0 }),
              Object.prototype.hasOwnProperty.call(this.#i, T) ||
                Object.defineProperty(this.#i, T, {
                  configurable: !0,
                  enumerable: !0,
                  get: () => X(C),
                  set: (O) => {
                    N([[C, O]]);
                  },
                }),
              X(C)
            );
          },
          {
            get: (T, y, C) =>
              typeof y != "string" || y in T
                ? Reflect.get(T, y, C)
                : g(`${this.#t}.${y}`),
            set: (T, y, C) => {
              if (typeof y != "string") return !1;
              let O = `${this.#t}.${y}`;
              return (
                Object.prototype.hasOwnProperty.call(this.#i, y) ||
                  Object.defineProperty(this.#i, y, {
                    configurable: !0,
                    enumerable: !0,
                    get: () => X(O),
                    set: (P) => {
                      N([[O, P]]);
                    },
                  }),
                N([[O, typeof C == "function" ? pe(C) : C]]),
                !0
              );
            },
            has: (T, y) =>
              typeof y != "string" ? y in T : y in T || m(`${this.#t}.${y}`),
            ownKeys: (T) => [
              ...new Set([
                ...Reflect.ownKeys(T),
                ...Object.keys(g(this.#t) ?? {}),
              ]),
            ],
            getOwnPropertyDescriptor: (T, y) => {
              if (Reflect.has(T, y))
                return Reflect.getOwnPropertyDescriptor(T, y);
              if (typeof y == "string" && m(`${this.#t}.${y}`))
                return {
                  configurable: !0,
                  enumerable: !0,
                  writable: !0,
                  value: g(`${this.#t}.${y}`),
                };
            },
          },
        ),
        h = new Proxy(
          {},
          {
            get: (T, y, C) =>
              typeof y == "string" ? this.#n[y] : Reflect.get({}, y, C),
            has: (T, y) => (typeof y != "string" ? !1 : y in this.#n),
            ownKeys: () => Object.keys(this.#n),
            getOwnPropertyDescriptor: (T, y) => {
              if (typeof y == "string" && y in this.#n)
                return {
                  configurable: !0,
                  enumerable: !0,
                  writable: !1,
                  value: this.#n[y],
                };
            },
          },
        ),
        S = {
          props: this.#r,
          $: Z,
          $$: f,
          effect: (T) => {
            let y = M(T);
            return this.#c.push(y), y;
          },
          apply: (T, y = !0) => {
            _e(T, y);
          },
          adoptStyles: (T, ...y) => {
            if (
              this.#e instanceof ShadowRoot &&
              "adoptedStyleSheets" in Document.prototype &&
              "replaceSync" in CSSStyleSheet.prototype
            ) {
              let O = y.filter(Boolean).map((P) => {
                let D = new CSSStyleSheet();
                return D.replaceSync(P), D;
              });
              this.#e.adoptedStyleSheets = [
                ...this.#e.adoptedStyleSheets,
                ...O,
              ];
              return;
            }
            let C = this.#e instanceof ShadowRoot ? this.#e : T;
            for (let O of y) {
              if (!O) continue;
              let P = document.createElement("style");
              (P.textContent = O), C.prepend(P);
            }
          },
          cleanup: (T) => {
            this.#c.push(T);
          },
          emit: (T, ...y) => {
            if (!y.length || typeof y[0] == "string") {
              for (let C of [T, ...y])
                this.dispatchEvent(new Event(C, { bubbles: !0, composed: !0 }));
              return;
            }
            this.dispatchEvent(
              new CustomEvent(T, {
                bubbles: !0,
                composed: !0,
                ...(y[1] ?? {}),
                detail: y[0],
              }),
            );
          },
          emitCancellable: (T, ...y) =>
            y.length
              ? this.dispatchEvent(
                  new CustomEvent(T, {
                    bubbles: !0,
                    composed: !0,
                    cancelable: !0,
                    ...(y[1] ?? {}),
                    detail: y[0],
                  }),
                )
              : this.dispatchEvent(
                  new Event(T, { bubbles: !0, composed: !0, cancelable: !0 }),
                ),
          actions: new Proxy(
            {},
            {
              get: (T, y) => {
                if (typeof y == "string")
                  return (...C) => {
                    let O = Fe[y];
                    if (!O) throw Error(`Undefined action: ${y}`);
                    return O(Sr(this, void 0, this.#h), ...C);
                  };
              },
            },
          ),
          action: (T, y) => {
            this.#g.set(T, y);
          },
          observeProps: (T, ...y) => {
            let C = { fn: T, names: y.length === 0 ? null : new Set(y) };
            return (
              this.#a.push(C),
              () => {
                let O = this.#a.indexOf(C);
                O >= 0 && this.#a.splice(O, 1);
              }
            );
          },
          overrideProp: (T, y, C) => {
            this.#L(T, y, C);
          },
          defineHostProp: (T, y) => {
            this.#N(T, y);
          },
          render: (T, ...y) => {
            this.#E(T, ...y);
          },
          host: this,
        },
        R = { ...S, refs: h };
      a?.(S),
        this.#E({}),
        this.#f && N([[this.#t, {}]], { ifMissing: !0 }),
        this.#p && (this.removeAttribute("data-ignore"), (this.#p = !1)),
        this.#e !== this && Mn(this),
        _e(this.#e, !0),
        (this.#y = !0),
        this.#o.push(...E(this.#e, { signalPathBase: this.#t })),
        this.#d();
      let k = new MutationObserver(() => this.#d());
      k.observe(this.#e, { childList: !0, subtree: !0 }),
        this.#e !== this && k.observe(this, { childList: !0, subtree: !0 }),
        this.#c.push(() => k.disconnect()),
        this.#C(),
        c?.(R);
    }
    attributeChangedCallback(f, h, S) {
      if (this.#T) return;
      let R = d.get(f);
      if (R) {
        if (S !== null && h === null && this.#S.get(f) === S) {
          this.#S.delete(f);
          return;
        }
        this.#k(R, S === null ? ue(l[R]) : Ee(l[R], S));
      }
    }
    disconnectedCallback() {
      N([[this.#t, null]]), this.removeEventListener(ve, this.#x);
      for (let f of this.#h.values()) f();
      this.#h.clear();
      for (let f of this.#l) f();
      this.#l.length = 0;
      for (let f of this.#o) f();
      this.#o.length = 0;
      for (let f of this.#c) f();
      this.#c.length = 0;
      for (let f of Object.keys(this.#n)) delete this.#n[f];
      (this.#a = []), this.#g.clear(), (this.#u = !1);
    }
    static {
      for (let f of u)
        Object.defineProperty(x.prototype, f, {
          get() {
            return this.#A(f);
          },
          set(h) {
            this.#w(f, h);
          },
        });
    }
  }
  return (
    Object.defineProperty(x, "manifest", {
      configurable: !1,
      enumerable: !1,
      value: () => wr(an.get(e) ?? p),
    }),
    customElements.define(e, x),
    an.set(e, p),
    x
  );
}
export {
  L as action,
  Fe as actions,
  A as attribute,
  F as beginBatch,
  pe as computed,
  or as createCodec,
  M as effect,
  H as endBatch,
  B as filtered,
  X as getPath,
  U as mergePatch,
  N as mergePaths,
  Er as publishRocketManifests,
  St as rocket,
  Z as root,
  Oe as signal,
  V as startPeeking,
  q as stopPeeking,
  He as watcher,
};
