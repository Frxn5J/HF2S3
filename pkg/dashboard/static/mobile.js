// Mobile helpers for the console.
// 1. Tables turn into cards on narrow screens (see "Mobile app shell" in style.css).
//    The cards need a label per cell, taken from the matching column header, so
//    rows rendered later by app.js are labelled as they are added.
// 2. Switching tab through the bottom bar starts the new page at the top.
(function () {
  'use strict';

  function labelRows(table) {
    const heads = Array.from(table.querySelectorAll('thead th')).map((th) => th.textContent.trim());
    if (!heads.length) return;
    table.querySelectorAll('tbody tr').forEach((tr) => {
      let col = 0;
      Array.from(tr.children).forEach((td) => {
        const span = td.colSpan || 1;
        const label = span === 1 ? heads[col] : '';
        if (label && td.getAttribute('data-label') !== label) td.setAttribute('data-label', label);
        col += span;
      });
    });
  }

  document.querySelectorAll('table.data-table').forEach((table) => {
    labelRows(table);
    const body = table.tBodies[0];
    if (!body) return;
    new MutationObserver(() => labelRows(table)).observe(body, { childList: true });
  });

  document.querySelectorAll('.nav-item').forEach((item) => {
    item.addEventListener('click', () => window.scrollTo(0, 0));
  });
})();
