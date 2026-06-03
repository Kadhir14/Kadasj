// =============================================================
// Shared cloud-sync helper. Each page calls initCloudSync({...}).
// =============================================================
(function () {
  'use strict';
  const SUPABASE_URL = 'https://cmanfkmodquxhvhhpckf.supabase.co';
  const SUPABASE_KEY = 'sb_publishable_wZX5R9CPMj91Jgh2m0QuUw_gk9VhlBi';

  window.initCloudSync = function (config) {
    const appKey        = config && config.appKey;
    const syncedKeys    = (config && config.syncedKeys)    || [];
    const syncedPrefixes = (config && config.syncedPrefixes) || [];
    const onApplied     = config && config.onApplied;
    if (!appKey || !window.supabase) return;
    if (!SUPABASE_URL || !SUPABASE_KEY) return;
    if (SUPABASE_URL.indexOf('PASTE-') === 0 || SUPABASE_KEY.indexOf('PASTE-') === 0) return;

    let supa           = null;
    let pushTimer      = null;
    let suppressSync   = false;
    let lastSyncedJson = null;
    let channel        = null;
    let pollTimer      = null;

    // ---- key matching ----
    function matches(k) {
      if (!k) return false;
      if (syncedKeys.indexOf(k) !== -1) return true;
      for (let i = 0; i < syncedPrefixes.length; i++) {
        if (k.indexOf(syncedPrefixes[i]) === 0) return true;
      }
      return false;
    }
    function listAllKeys() {
      const out = [];
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (matches(k)) out.push(k);
      }
      return out;
    }
    function collect() {
      const out = {};
      for (const k of listAllKeys()) {
        const v = localStorage.getItem(k);
        if (v == null) continue;
        try { out[k] = JSON.parse(v); } catch (e) { out[k] = v; }
      }
      return out;
    }

    // ---- localStorage intercept ----
    const origSet    = localStorage.setItem.bind(localStorage);
    const origRemove = localStorage.removeItem.bind(localStorage);
    localStorage.setItem = function (k, v) {
      origSet(k, v);
      try { if (!suppressSync && matches(k)) schedulePush(); } catch (e) {}
    };
    localStorage.removeItem = function (k) {
      origRemove(k);
      try { if (!suppressSync && matches(k)) schedulePush(); } catch (e) {}
    };

    // ---- apply remote state locally ----
    function applyRemote(remote) {
      if (!remote || typeof remote !== 'object') return false;
      suppressSync = true;
      let changed = false;
      try {
        for (const k of Object.keys(remote)) {
          if (!matches(k)) continue;
          const incoming = JSON.stringify(remote[k]);
          const local = localStorage.getItem(k);
          if (local !== incoming) { try { origSet(k, incoming); changed = true; } catch (e) {} }
        }
        for (const k of listAllKeys()) {
          if (!(k in remote)) { try { origRemove(k); changed = true; } catch (e) {} }
        }
      } finally { suppressSync = false; }
      if (changed && typeof onApplied === 'function') { try { onApplied(); } catch (e) {} }
      return changed;
    }

    // ---- push local state to Supabase ----
    async function pushNow() {
      if (!supa) return;
      const state = collect();
      const json  = JSON.stringify(state);
      if (json === lastSyncedJson) return;
      try {
        const { error } = await supa.from('app_state').upsert(
          { key: appKey, data: state, updated_at: new Date().toISOString() },
          { onConflict: 'key' }
        );
        if (!error) lastSyncedJson = json;
      } catch (e) {}
    }

    // Push 50 ms after the last change (was 250 ms — 5× faster trigger)
    function schedulePush() {
      clearTimeout(pushTimer);
      pushTimer = setTimeout(pushNow, 50);
    }

    // keepalive fetch on page hide so the push survives navigation
    function flushOnUnload() {
      const state = collect();
      const json  = JSON.stringify(state);
      if (json === lastSyncedJson) return;
      try {
        fetch(SUPABASE_URL + '/rest/v1/app_state?on_conflict=key', {
          method: 'POST',
          headers: {
            'apikey': SUPABASE_KEY,
            'Authorization': 'Bearer ' + SUPABASE_KEY,
            'Content-Type': 'application/json',
            'Prefer': 'resolution=merge-duplicates',
          },
          body: JSON.stringify({ key: appKey, data: state, updated_at: new Date().toISOString() }),
          keepalive: true,
        }).catch(() => {});
        lastSyncedJson = json;
      } catch (e) {}
    }

    // ---- poll Supabase for the latest state ----
    // Used as a fallback when the realtime WebSocket has gone quiet
    // (e.g. after the mobile OS backgrounds the browser).
    async function pollRemote() {
      if (!supa) return;
      try {
        const { data, error } = await supa
          .from('app_state')
          .select('data')
          .eq('key', appKey)
          .maybeSingle();
        if (!error && data && data.data) {
          const incoming = JSON.stringify(data.data);
          if (incoming !== lastSyncedJson) {
            lastSyncedJson = incoming;
            applyRemote(data.data);
          }
        }
      } catch (e) {}
    }

    // ---- realtime subscription (extracted so we can reconnect) ----
    function subscribeRealtime() {
      // Remove any stale channel before creating a new one
      if (channel) {
        try { supa.removeChannel(channel); } catch (e) {}
        channel = null;
      }
      channel = supa
        .channel('app_state_' + appKey + '_' + Date.now()) // unique name avoids duplicate-channel errors
        .on('postgres_changes', {
          event: '*',
          schema: 'public',
          table: 'app_state',
          filter: 'key=eq.' + appKey,
        }, (payload) => {
          if (!payload.new || !payload.new.data) return;
          const incoming = JSON.stringify(payload.new.data);
          if (incoming === lastSyncedJson) return;
          lastSyncedJson = incoming;
          applyRemote(payload.new.data);
        })
        .subscribe();
    }

    // ---- startup ----
    (async function init() {
      supa = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);

      // Initial pull
      try {
        const { data, error } = await supa
          .from('app_state')
          .select('data')
          .eq('key', appKey)
          .maybeSingle();
        if (!error && data && data.data && Object.keys(data.data).length > 0) {
          lastSyncedJson = JSON.stringify(data.data);
          applyRemote(data.data);
        } else if (Object.keys(collect()).length > 0) {
          schedulePush();
        }
      } catch (e) {}

      subscribeRealtime();
    })();

    // Poll every 4 seconds — catches updates the realtime channel missed.
    // Short enough to feel near-instant; long enough not to hammer the DB.
    pollTimer = setInterval(pollRemote, 4000);

    // When the user switches back to this tab / brings the app to the
    // foreground, pull immediately AND reconnect the WebSocket (mobile OSes
    // kill WebSocket connections when a browser tab is backgrounded).
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) return;
      pollRemote();
      subscribeRealtime();
    });

    // Pull immediately when the browser window regains focus
    window.addEventListener('focus', pollRemote);

    window.addEventListener('beforeunload', flushOnUnload);
    window.addEventListener('pagehide',     flushOnUnload);
    window.addEventListener('storage', (e) => { if (e.key && matches(e.key)) schedulePush(); });
  };
})();
