/* =========================================================
   HOODSTREET — Premium Interactions
========================================================= */

const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

const body = document.body;
const entrance = $("#entrance");
const site = $("#site");
const enterBtn = $("#enterBtn");
const menuBtn = $("#menuBtn");
const mobileMenu = $("#mobileMenu");
const cursorGlow = $("#cursorGlow");


/* =========================================================
   AUDIO ENGINE — Web Audio API (no files needed)
========================================================= */

const AudioEngine = (() => {
  let ctx = null;
  let enabled = true;
  let initialized = false;

  function init() {
    if (initialized) return;
    try {
      ctx = new (window.AudioContext || window.webkitAudioContext)();
      initialized = true;
    } catch (e) {
      console.warn("Web Audio API not supported");
    }
  }

  function ensureContext() {
    if (!ctx) init();
    if (ctx && ctx.state === "suspended") ctx.resume();
    return ctx && enabled;
  }

  // Deep cinematic bass thud — entrance
  function playEntrance() {
    if (!ensureContext()) return;
    const now = ctx.currentTime;

    // Sub bass
    const osc1 = ctx.createOscillator();
    const gain1 = ctx.createGain();
    osc1.type = "sine";
    osc1.frequency.setValueAtTime(60, now);
    osc1.frequency.exponentialRampToValueAtTime(30, now + 0.5);
    gain1.gain.setValueAtTime(0.4, now);
    gain1.gain.exponentialRampToValueAtTime(0.001, now + 0.8);
    osc1.connect(gain1).connect(ctx.destination);
    osc1.start(now);
    osc1.stop(now + 0.8);

    // Mid impact
    const osc2 = ctx.createOscillator();
    const gain2 = ctx.createGain();
    osc2.type = "sine";
    osc2.frequency.setValueAtTime(120, now);
    osc2.frequency.exponentialRampToValueAtTime(40, now + 0.3);
    gain2.gain.setValueAtTime(0.25, now);
    gain2.gain.exponentialRampToValueAtTime(0.001, now + 0.4);
    osc2.connect(gain2).connect(ctx.destination);
    osc2.start(now);
    osc2.stop(now + 0.4);

    // Noise burst
    const bufferSize = ctx.sampleRate * 0.15;
    const buffer = ctx.createBuffer(1, bufferSize, ctx.sampleRate);
    const data = buffer.getChannelData(0);
    for (let i = 0; i < bufferSize; i++) {
      data[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / bufferSize, 3);
    }
    const noise = ctx.createBufferSource();
    noise.buffer = buffer;
    const noiseGain = ctx.createGain();
    noiseGain.gain.setValueAtTime(0.08, now);
    noiseGain.gain.exponentialRampToValueAtTime(0.001, now + 0.15);
    const noiseFilter = ctx.createBiquadFilter();
    noiseFilter.type = "lowpass";
    noiseFilter.frequency.value = 800;
    noise.connect(noiseFilter).connect(noiseGain).connect(ctx.destination);
    noise.start(now);
    noise.stop(now + 0.15);

    // Haptic
    if (navigator.vibrate) navigator.vibrate([30, 20, 50]);
  }

  // Satisfying click — buttons
  function playClick() {
    if (!ensureContext()) return;
    const now = ctx.currentTime;

    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(800, now);
    osc.frequency.exponentialRampToValueAtTime(400, now + 0.06);
    gain.gain.setValueAtTime(0.15, now);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.08);
    osc.connect(gain).connect(ctx.destination);
    osc.start(now);
    osc.stop(now + 0.08);

    // Click transient
    const osc2 = ctx.createOscillator();
    const gain2 = ctx.createGain();
    osc2.type = "square";
    osc2.frequency.setValueAtTime(1200, now);
    gain2.gain.setValueAtTime(0.06, now);
    gain2.gain.exponentialRampToValueAtTime(0.001, now + 0.02);
    osc2.connect(gain2).connect(ctx.destination);
    osc2.start(now);
    osc2.stop(now + 0.03);
  }

  // Subtle swoosh — hover
  function playSwoosh() {
    if (!ensureContext()) return;
    const now = ctx.currentTime;

    const bufferSize = ctx.sampleRate * 0.12;
    const buffer = ctx.createBuffer(1, bufferSize, ctx.sampleRate);
    const data = buffer.getChannelData(0);
    for (let i = 0; i < bufferSize; i++) {
      const t = i / bufferSize;
      data[i] = (Math.random() * 2 - 1) * Math.sin(t * Math.PI) * 0.3;
    }
    const source = ctx.createBufferSource();
    source.buffer = buffer;

    const filter = ctx.createBiquadFilter();
    filter.type = "bandpass";
    filter.frequency.setValueAtTime(2000, now);
    filter.frequency.exponentialRampToValueAtTime(6000, now + 0.1);
    filter.Q.value = 0.5;

    const gain = ctx.createGain();
    gain.gain.setValueAtTime(0.04, now);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.12);

    source.connect(filter).connect(gain).connect(ctx.destination);
    source.start(now);
  }

  // Stamp / seal impact — form success
  function playStamp() {
    if (!ensureContext()) return;
    const now = ctx.currentTime;

    // Impact
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(150, now);
    osc.frequency.exponentialRampToValueAtTime(50, now + 0.3);
    gain.gain.setValueAtTime(0.35, now);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.4);
    osc.connect(gain).connect(ctx.destination);
    osc.start(now);
    osc.stop(now + 0.4);

    // Paper crunch
    const bufferSize = ctx.sampleRate * 0.2;
    const buffer = ctx.createBuffer(1, bufferSize, ctx.sampleRate);
    const data = buffer.getChannelData(0);
    for (let i = 0; i < bufferSize; i++) {
      const t = i / bufferSize;
      data[i] = (Math.random() * 2 - 1) * Math.pow(1 - t, 4) * 0.5;
    }
    const noise = ctx.createBufferSource();
    noise.buffer = buffer;
    const noiseGain = ctx.createGain();
    noiseGain.gain.setValueAtTime(0.12, now);
    noiseGain.gain.exponentialRampToValueAtTime(0.001, now + 0.2);
    const filter = ctx.createBiquadFilter();
    filter.type = "highpass";
    filter.frequency.value = 2000;
    noise.connect(filter).connect(noiseGain).connect(ctx.destination);
    noise.start(now);
    noise.stop(now + 0.2);

    // Haptic
    if (navigator.vibrate) navigator.vibrate([40, 30, 60, 20, 30]);
  }

  // Success chime — subtle confirmation
  function playSuccess() {
    if (!ensureContext()) return;
    const now = ctx.currentTime;

    [523, 659, 784].forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0, now + i * 0.12);
      gain.gain.linearRampToValueAtTime(0.08, now + i * 0.12 + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.001, now + i * 0.12 + 0.3);
      osc.connect(gain).connect(ctx.destination);
      osc.start(now + i * 0.12);
      osc.stop(now + i * 0.12 + 0.3);
    });
  }

  return {
    init,
    get enabled() { return enabled; },
    set enabled(v) { enabled = v; },
    playEntrance,
    playClick,
    playSwoosh,
    playStamp,
    playSuccess
  };
})();

// Initialize audio on first user interaction
document.addEventListener("click", () => AudioEngine.init(), { once: true });
document.addEventListener("keydown", () => AudioEngine.init(), { once: true });


/* =========================================================
   ENTRANCE
========================================================= */

function enterHoodStreet() {
  AudioEngine.playEntrance();
  entrance.classList.add("leave");
  site.classList.add("visible");
  site.setAttribute("aria-hidden", "false");
  body.classList.remove("entrance-active");

  setTimeout(() => {
    entrance.style.display = "none";
  }, 1600);
}

enterBtn.addEventListener("click", enterHoodStreet);

// Skip entrance on desktop with spacebar
document.addEventListener("keydown", (e) => {
  if (e.code === "Space" && entrance && !entrance.classList.contains("leave")) {
    e.preventDefault();
    enterHoodStreet();
  }
});


/* =========================================================
   DOSSIER HOVER SOUNDS
========================================================= */

$$(".dossier").forEach(dossier => {
  dossier.addEventListener("mouseenter", () => AudioEngine.playSwoosh());
});

// Nav link hover sounds
$$(".desktop-nav a").forEach(link => {
  link.addEventListener("mouseenter", () => AudioEngine.playClick());
});


/* =========================================================
   AUDIO TOGGLE
========================================================= */

const audioToggle = $("#audioToggle");
const audioToggleMobile = $("#audioToggleMobile");

function toggleAudio() {
  AudioEngine.enabled = !AudioEngine.enabled;
  const muted = !AudioEngine.enabled;
  if (audioToggle) {
    audioToggle.classList.toggle("muted", muted);
    audioToggle.setAttribute("aria-label",
      muted ? "Unmute sounds" : "Mute sounds"
    );
  }
  if (audioToggleMobile) {
    audioToggleMobile.classList.toggle("muted", muted);
    audioToggleMobile.setAttribute("aria-label",
      muted ? "Unmute sounds" : "Mute sounds"
    );
  }
  AudioEngine.playClick();
}

if (audioToggle) audioToggle.addEventListener("click", toggleAudio);
if (audioToggleMobile) audioToggleMobile.addEventListener("click", toggleAudio);


/* =========================================================
   MOBILE MENU
========================================================= */

function toggleMenu() {
  AudioEngine.playClick();
  const isOpen = mobileMenu.classList.toggle("open");
  menuBtn.classList.toggle("active", isOpen);
  menuBtn.setAttribute("aria-expanded", isOpen);
  body.style.overflow = isOpen ? "hidden" : "";
}

menuBtn.addEventListener("click", toggleMenu);

$$(".mobile-menu a").forEach(link => {
  link.addEventListener("click", () => {
    mobileMenu.classList.remove("open");
    menuBtn.classList.remove("active");
    menuBtn.setAttribute("aria-expanded", "false");
    body.style.overflow = "";
  });
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && mobileMenu.classList.contains("open")) {
    toggleMenu();
  }
});


/* =========================================================
   VIDEO PLAYER
========================================================= */

const teaserVideo = $("#teaserVideo");
const playVideo = $("#playVideo");
const filmOverlay = $("#filmOverlay");
const filmFrame = $("#filmFrame");

if (playVideo && teaserVideo) {
  playVideo.addEventListener("click", async (e) => {
    e.stopPropagation();
    AudioEngine.playClick();
    try {
      await teaserVideo.play();
      filmOverlay.classList.add("hide");
    } catch (err) {
      console.log("Playback blocked:", err);
    }
  });

  filmFrame.addEventListener("click", () => {
    if (teaserVideo.paused) {
      teaserVideo.play();
      filmOverlay.classList.add("hide");
    } else {
      teaserVideo.pause();
      filmOverlay.classList.remove("hide");
    }
  });

  teaserVideo.addEventListener("ended", () => {
    filmOverlay.classList.remove("hide");
  });
}


/* =========================================================
   SCROLL REVEALS
========================================================= */

const revealObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add("visible");
        revealObserver.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.1, rootMargin: "0px 0px -40px 0px" }
);

$$(".reveal").forEach(el => revealObserver.observe(el));


/* =========================================================
   CURSOR GLOW (desktop only)
========================================================= */

if (window.matchMedia("(pointer: fine)").matches && cursorGlow) {
  let mouseX = 0, mouseY = 0;
  let glowX = 0, glowY = 0;

  document.addEventListener("mousemove", (e) => {
    mouseX = e.clientX;
    mouseY = e.clientY;
  });

  function animateGlow() {
    glowX += (mouseX - glowX) * 0.08;
    glowY += (mouseY - glowY) * 0.08;
    cursorGlow.style.left = glowX + "px";
    cursorGlow.style.top = glowY + "px";
    requestAnimationFrame(animateGlow);
  }

  animateGlow();
}


/* =========================================================
   NAVBAR SCROLL EFFECT
========================================================= */

const navbar = $(".navbar");
let lastScroll = 0;

window.addEventListener("scroll", () => {
  const scrollY = window.scrollY;

  if (scrollY > 100) {
    navbar.style.background =
      "linear-gradient(180deg, rgba(10,10,10,.98) 0%, rgba(10,10,10,.95) 80%, rgba(10,10,10,.85) 100%)";
  } else {
    navbar.style.background =
      "linear-gradient(180deg, rgba(10,10,10,.95) 0%, rgba(10,10,10,.7) 60%, transparent 100%)";
  }

  lastScroll = scrollY;
}, { passive: true });


/* =========================================================
   PARALLAX — Hero background text
========================================================= */

const heroBgText = $(".hero-bg-text");

if (heroBgText) {
  window.addEventListener("scroll", () => {
    const scrollY = window.scrollY;
    if (scrollY < window.innerHeight) {
      heroBgText.style.transform =
        `translate(-50%, calc(-50% + ${scrollY * 0.15}px))`;
    }
  }, { passive: true });
}


/* =========================================================
   WHITELIST FORM
========================================================= */

const whitelistForm = $("#whitelistForm");
const applicationFormWrap = $("#applicationFormWrap");
const successState = $("#successState");
const submitBtn = $(".submit-btn");

if (submitBtn) {
  submitBtn.addEventListener("mouseenter", () => AudioEngine.playSwoosh());
}

if (whitelistForm) {
  whitelistForm.addEventListener("submit", (e) => {
    e.preventDefault();

    /*
      V1 Demo Mode — does not transmit data.
      Replace with actual backend after design approval.
    */

    AudioEngine.playStamp();

    setTimeout(() => {
      applicationFormWrap.style.display = "none";
      successState.classList.add("visible");
      AudioEngine.playSuccess();
    }, 300);
  });
}


/* =========================================================
   SMOOTH ANCHOR SCROLL
========================================================= */

$$('a[href^="#"]').forEach(anchor => {
  anchor.addEventListener("click", (e) => {
    const target = $(anchor.getAttribute("href"));
    if (target) {
      e.preventDefault();
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });
});
