// Blocks pinch zoom on touch devices. The viewport meta already asks for it,
// but iOS Safari ignores user-scalable=no, so the gesture is cancelled here too.
// Desktop browsers (fine pointer) keep their normal zoom.
//
// There is deliberately no touchend double-tap canceller: it swallows the second
// click of a quick double press. Double-tap zoom is disabled in CSS instead
// (touch-action: manipulation).
(function () {
  'use strict';

  if (!window.matchMedia || !window.matchMedia('(pointer: coarse)').matches) return;

  const stop = (e) => e.preventDefault();

  // iOS Safari pinch events.
  ['gesturestart', 'gesturechange', 'gestureend'].forEach((name) => {
    document.addEventListener(name, stop, { passive: false });
  });

  // Other engines: cancel any move that involves more than one finger.
  document.addEventListener(
    'touchmove',
    (e) => {
      if (e.touches.length > 1) e.preventDefault();
    },
    { passive: false }
  );
})();
