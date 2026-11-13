// Tiny canvas charts (no external libraries, so the UI works on an offline LAN).
function css(v) { return getComputedStyle(document.documentElement).getPropertyValue(v).trim(); }

function drawHistogram(canvas, probs) {
  const g = canvas.getContext("2d"), W = canvas.width, H = canvas.height, bins = 10, n = new Array(bins).fill(0);
  g.clearRect(0, 0, W, H);
  probs.forEach(p => n[Math.min(bins - 1, Math.floor(p * bins))]++);
  const max = Math.max(1, ...n), bw = (W - 60) / bins;
  g.font = "12px sans-serif"; g.fillStyle = css("--mut"); g.fillText("Spam probability of recent emails", 10, 16);
  n.forEach((c, i) => {
    const h = (H - 60) * c / max;
    g.fillStyle = i >= 5 ? css("--spam") : css("--ham");
    g.fillRect(40 + i * bw + 3, H - 30 - h, bw - 6, h);
    g.fillStyle = css("--mut"); g.fillText((i / bins).toFixed(1), 40 + i * bw, H - 12);
    if (c) g.fillText(c, 40 + i * bw + bw / 2 - 4, H - 34 - h);
  });
}

function drawConfusion(canvas, cm) {
  const g = canvas.getContext("2d"), cell = 110, ox = 110, oy = 50;
  g.clearRect(0, 0, canvas.width, canvas.height);
  g.font = "13px sans-serif"; g.fillStyle = css("--mut");
  g.fillText("Predicted ham", ox + 10, 40); g.fillText("Predicted spam", ox + cell + 6, 40);
  g.fillText("Actual ham", 20, oy + 60); g.fillText("Actual spam", 14, oy + cell + 60);
  const cells = [[cm.tn, true], [cm.fp, false], [cm.fn, false], [cm.tp, true]];
  const max = Math.max(1, ...cells.map(c => c[0]));
  cells.forEach(([v, good], i) => {
    const x = ox + (i % 2) * cell, y = oy + Math.floor(i / 2) * cell;
    g.globalAlpha = 0.15 + 0.7 * v / max; g.fillStyle = good ? css("--ham") : css("--spam"); g.fillRect(x, y, cell - 4, cell - 4);
    g.globalAlpha = 1; g.fillStyle = css("--ink"); g.font = "600 20px sans-serif"; g.fillText(v, x + 36, y + 60);
  });
}
