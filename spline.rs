use glam::Vec2;

// All of the functionality below still needs tests, but I think it's been ported faithfully
// from the original sources in JavaScript and Zig.
// todo: check intuitive-ink-early-prototyping/bez.zig for some code for emitting the polyline itself.
// perhaps we can do better this time, but that should make for a good start.
// todo: debug_assert expected properties
// todo: test the quad bez sampler for n=0 (should emit no points)
// todo: fuzz test subdivision/sampler to ensure that it keeps to the required error tolerance
// todo: fuzz test spline somehow... not sure how since the derivative condition is what needs testing
//       we can at least test that the control points that should align with the input points do so.

/// Constructs a composite quadratic Bezier curve by approximating a Catmull-Rom spline through
/// the given points using a technique from the paper `Quad Approximation of Cubic Curves`,
/// available in `/refs/Yuksel - Quadratic Approximation of Cubic Curves.pdf`.
/// All but the first and last control point will be interpolated by the curve.
/// This implementation uses Catmull-Rom α = 0.5 for centripetal Catmull-Rom splines:
///   <https://en.wikipedia.org/wiki/Centripetal_Catmull%E2%80%93Rom_spline>
/// and uses γ = 0.5 as the parametric position of the splitting point to minimize
/// the deviation due to degree reduction. This can be thought of as joining the
/// quadratic curves exactly at the center of the cubic bezier they approximate.
///
/// This implementation minimizes the number of square root calls by re-using computations
/// as we move over windows of four control points at a time. See <https://yuri.is/splines/>
pub struct Spline {
    /// Points
    p: [Vec2; 3],
    /// Distances between points
    d: [f32; 2],
    /// Square roots of distances between points
    s: [f32; 2],
}

impl Spline {
    /// Construct a spline with initial control points `p0`, `p1`, and `p2`.
    /// At the next call to `point(p3)`, the curve will interpolate through `p1` and `p2`
    /// while using `p0` and `p3` as control points.
    /// To draw a curve through _start_ and _end_, use _start_ as `p0` and `p1` in `new`, and
    /// place an additional call to `point(end)` to use the endpoint as the final control point.
    pub fn new(p0: Vec2, p1: Vec2, p2: Vec2) -> Spline {
        let p = [p0, p1, p2];
        let d = [p0.distance(p1), p1.distance(p2)];
        let s = d.map(f32::sqrt);
        Spline { p, d, s }
    }

    /// Add a control point to this spline and emit two quadratic beziers that interpolate between
    /// p1 and p2, using p0 and p3 for control information.
    /// The return to quadratic curves share a control point: the final point of the first curve
    /// is the same as the first point of the second curve. The return value
    ///   [q00, q01, q02, q11, q12]
    /// represents two quadratic bezier curves with the following control points:
    ///   [q00, q01, q02]
    ///   [q10, q11, q12]
    /// where
    ///
    ///   - q10 == q02
    ///   - q00 is initially `p1` (as passed to `new`) and afterwards `q12` from the previous `point` invocation
    ///   - q12 is initially `p2` (as passed to `new`) and afterwards `p3` from the previous `point` invocation
    ///
    /// In the future we can explore returning only [q01, q02, q11] since the other values could be cached by the caller.
    /// This would reduce unnecessary data movement. The API that returns all of the control points seems friendlier, though.
    pub fn point(&mut self, p3: Vec2) -> [Vec2; 5] {
        let Spline {
            p: [p0, p1, p2],
            d: [d01, d12],
            s: [s01, s12],
        } = *self;

        let d23 = p2.distance(p3);
        let s23 = d23.sqrt();

        // Calculate q01, avoiding divide-by-zero when the denominator is zero
        let q01 = if s01 == 0.0 {
            p1
        } else {
            let num = (p2 - p1) * d01 + (p1 - p0) * d12;
            let den = 4.0 * s01 * (s01 + s12);
            p1 + num / den
        };

        // Calculate q11, avoiding divide-by-zero when the denominator is zero
        let q11 = if s23 == 0.0 {
            p2
        } else {
            let num = (p2 - p3) * d12 + (p1 - p2) * d23;
            let den = 4.0 * s23 * (s12 + s23);
            p2 + num / den
        };

        // The shared common point is the mean of the two control points.
        // The value for the first control point of the second curve (q10)
        // is equal to the last control point of the first curve (q02).
        let q02 = 0.5 * (q01 + q11); // todo: use the upcoming .midpoint() method

        // Update internal state for the next iteration
        *self = Spline {
            p: [p1, p2, p3],
            d: [d12, d23],
            s: [s12, s23],
        };

        let q00 = p1;
        let q12 = p2;
        [q00, q01, q02, q11, q12]
    }
}

/// Approximate a cubic bezier segment by two quadratic bezier segments.
/// The input arguments are the control points for the cubic bezier segment.
/// See the `Quad Approximation of Cubic Curves` paper and the accompanying
/// Mathematica notebook referenced here: <https://yuri.is/splines/>
/// Both source materials are available in the refs folder:
///   `/refs/Yuksel - Quadratic Approximation of Cubic Curves.pdf` and
///   `/refs/CatmullRomQuadraticApproximation.nb`.
/// The quadratic Bezier segments share a control point. The return value
///   [q00, q01, q02, q11, q12]
/// represents two quadratic bezier curves with the following control points:
///   [q00, q01, q02]
///   [q10, q11, q12]
/// where q10 == q02.
pub fn cubic_bezier_to_quadratic_beziers(p0: Vec2, p1: Vec2, p2: Vec2, p3: Vec2) -> [Vec2; 5] {
    let a = 3.0 * p1 + p0;
    let b = 3.0 * p2 + p3;
    let q00 = p0;
    let q01 = 0.25 * a;
    let q02 = 0.125 * (a + b);
    let q11 = 0.25 * b;
    let q12 = p3;
    [q00, q01, q02, q11, q12]
}

/// Number of equally-spaced line segments to subdivide the quadratic bezier into
/// such that the maximum distance that the curve deviates from the line segments
/// is bounded by the provided max distance (error tolerance). The method is described in
/// Section 10.6 of `/refs/Sederberg - Computer Aided Geometric Design.pdf`,
/// with the denominator corrected to 4 (the original suggests 8, which can yield results
/// above the error tolerance). I discovered a reference to this method here:
/// <https://raphlinus.github.io/graphics/curves/2019/12/23/flatten-quadbez.html>
/// See also: <https://github.com/raphlinus/raphlinus.github.io/issues/57> where I noticed
/// that Raph used a multiple of 4 rather than 8 as in the paper for his implementation.
/// See also: <https://observablehq.com/d/fa7a7d536c24e4d4> where I verified through
/// randomized testing that 4 (but not 8!) gives results within the `max_dist` tolerance.
/// `tol` is the maximum distance that is tolerated for the deviation of a polyline point
/// away from the true curve.
pub fn quadratic_bezier_subdivisions(p0: Vec2, p1: Vec2, p2: Vec2, tol: f32) -> u32 {
    let dd = (2.0 * p1 - p0 - p2).length();
    let n = (dd / (4.0 * tol)).sqrt().ceil() as u32;
    // always subdivide into at least 1 line segment, and, for sanity,
    // never subdivide a quadratic bezier into more than a small number
    // of divisions. This prevents degenerate cases.
    n.clamp(1, 100)
}

/// Evaluate a quadratic bezier at n evenly-spaced steps using forward differencing.
/// See Chapter 4 of `/refs/Sederberg - Computer Aided Geometric Design.pdf`.
/// See also `/refs/Bartley - Forward Difference Calculation of Bezier Curves.html`.
pub struct QuadraticBezierSampler {
    /// Current iteration number
    i: u32,
    /// Desired number of samples
    n: u32,
    /// Current point
    current: Vec2,
    /// Final point at t=1, stored so that it can be emitted exactly
    endpoint: Vec2,
    /// First forward difference
    diff1: Vec2,
    /// Second forward difference
    diff2: Vec2,
}

impl QuadraticBezierSampler {
    /// Construct a new iterator that will evaluate the quadratic bezier curve at `n`
    /// evenly-spaced steps. The control points of the curve are `p0`, `p1`, and `p2`.
    /// The sampling will be evenly spaced between t=0 and t=1.
    ///
    /// The initial point at t=0 is never emitted, and the final point at t=1 is emitted exactly.
    pub fn new(p0: Vec2, p1: Vec2, p2: Vec2, n: u32) -> QuadraticBezierSampler {
        // Coefficients for p(t) = at^2 + bt + c
        let a = p0 - (2.0 * p1) + p2;
        let b = -2.0 * (p0 - p1);
        let c = p0; // Initial value at time t=0

        // h = step size
        let h = 1.0 / f64::from(n);
        let h_sq = (h * h) as f32;
        let h = h as f32;

        // Initial forward difference to step along the quadratic
        let diff1 = h_sq * a + h * b;

        // Initial forward difference to step along ah^2
        let diff2 = a * 2.0 * h_sq;

        QuadraticBezierSampler {
            i: 0,
            n,
            current: c,
            endpoint: p2,
            diff1,
            diff2,
        }
    }
}

impl Iterator for QuadraticBezierSampler {
    type Item = Vec2;

    /// Step incrementally along the quadratic curve, emitting samples as we go.
    ///
    /// - We never evaluate at t=0, so the first point is not emitted.
    /// - We always evaluate at t=1, and this final point is always emitted exactly.
    ///
    /// This behavior is useful for chaining connected quadratic beziers where the
    /// first point of the current curve is redundant with the last point of the previous curve.
    fn next(&mut self) -> Option<Vec2> {
        self.i += 1;
        match self.i.cmp(&self.n) {
            // Emit incremental samples when i < n
            // todo: for clarity, refactor this to use ..n, n, and _ as the 3 match prongs
            // once nonexclusive range pattern-matching syntax lands.
            core::cmp::Ordering::Less => {
                self.current += self.diff1;
                self.diff1 += self.diff2;
                Some(self.current)
            }
            // Emit the exact endpoint at t=1
            core::cmp::Ordering::Equal => Some(self.endpoint),
            core::cmp::Ordering::Greater => None,
        }
    }
}

pub struct PolylineTesselator {
    // Perpendicular to the line from `prev` to `pos`
    pos: Vec2,
    perp: Vec2,
    prev_pos: Vec2,
    prev_perp: Vec2,
}

// Lines to triangles
// Implementation based on my previous implementation in bez.zig.
// We don't use the stroke width in this implementation, since offsetting
// is now done on the GPU.
impl PolylineTesselator {
    pub fn new(pos: Vec2) -> Self {
        Self {
            pos,
            perp: Vec2::ZERO,
            prev_pos: pos,
            prev_perp: Vec2::ZERO,
        }
    }

    /// Returns Some((pos, perp)) if any points were emitted and otherwise None,
    /// in order to aavoid emitting the same point twice in a row.
    /// Given the next point, we either want to emit nothing (if it's
    /// the same or very close to the previous point), or we want to
    /// emit a pair of points that are perpendicular to the previous
    /// point. This function will compute `pos` and `perp` and return
    /// an indication of whether any points should be emitted.
    /// Don't forget to call finalPoint to emit the last point, since
    /// this function always waits to emit points perpendicular to the
    // first until the second point is seen.
    pub fn point(&mut self, next: Vec2) -> Option<(Vec2, Vec2)> {
        // Avoid emitting the same point twice in a row
        if self.pos == next {
            return None;
        }
        let next_perp = (self.pos - next).normalize().perp();

        // Compute the midpoint of prevPerp and nextPerp, using this.perp as storage.
        // If the new perp is just a reflection of the old perp,
        // eg. for the middle point of the polyline (5, 5) (10, 5), (9, 5),
        // break the tie rather than emitting NaNs.
        let perp = self.prev_perp.midpoint(next_perp);
        // todo: should we check the squared distance versus an epsilon instead?
        self.perp = if perp == Vec2::ZERO {
            self.prev_perp
        } else {
            perp.normalize()
        };

        self.prev_perp = next_perp;
        self.prev_pos = self.pos;
        self.pos = next;
        Some((self.pos, self.perp))
    }

    // Reflect the previous point around the current point, and use
    // that to determine the extrusion direction for the final point.
    pub fn final_point(&mut self) -> Option<(Vec2, Vec2)> {
        self.point(self.pos + (self.pos - self.prev_pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quadratic_bezier_sampler() {
        // test that the sampler does not emit the first point
        // and emits the last point exactly
        let p0 = Vec2::new(0.0, 0.0);
        let p1 = Vec2::new(1.0, 2.0);
        let p2 = Vec2::new(3.0, 4.0);
        let mut s = QuadraticBezierSampler::new(p0, p1, p2, 3);
        assert_ne!(p0, s.next().unwrap());
        s.next().unwrap();
        assert_eq!(p2, s.next().unwrap());
        assert_eq!(s.next(), None);

        // emits no points when n = 0
        QuadraticBezierSampler::new(p0, p1, p2, 0);
        assert_eq!(s.next(), None);
    }
}
