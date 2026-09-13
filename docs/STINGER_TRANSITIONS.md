# Stinger Transitions

> **Code is the source of truth.** This guide describes intended behaviour and may have
> drifted; read the code for the current implementation.

A stinger transition plays a piece of branded motion graphics over the program bus while
the program source changes underneath it. The graphic covers the frame, the sources swap
behind it, and the graphic plays out — so the audience sees the graphic, not the cut.

The graphic can be a video clip with an alpha channel, or a web page animated with
HTML, CSS and JavaScript.

## Setting one up

With a clip:

1. Add a **Media Player** block and put your stinger clip in its playlist.
2. Turn on **Stinger Clip Source** on that block.
3. Wire its video output into one of the **Vision Mixer**'s DSK (keyed) inputs.

With a web page:

1. Add an **HTML Graphic** block and set its **URL** to the page.
2. Turn on **Stinger Source**, and set **Stinger Duration** to how long the page's
   animation runs.
3. Wire its video output into one of the **Vision Mixer**'s DSK inputs, and set that
   input's **DSK Alpha** to **Premultiplied**.

Declaring the source is not optional and is not implied by the wiring. Declaring a player
as a stinger source changes how it behaves: its clip is held on the first frame ready to
fire, and looping is switched off so it plays once per trigger. A media player wired to a
keyed input *without* that switch is left completely alone, which is what lets you keep a
looping graphic on a keyed input alongside a stinger.

## Timing the stinger

These properties live on the source block because they follow from the artwork: whoever
made the graphic knows where it covers, and an operator should not be retyping that under
time pressure.

- **Stinger Cut Point** — how far into the graphic the program source actually changes.
  Set it to the moment the graphic fully covers the frame. Leave it at 0 and the halfway
  point is used.
- **Stinger Beneath** — what the program does at the cut point. A cut is what a fully
  covering graphic wants. One that does not completely cover the frame is a good reason to
  put a wipe or a mix underneath instead.
- **Stinger Beneath Duration** — how long that transition beneath takes. It is *not* the
  length of the stinger. It is ignored for a cut.
- **Stinger Duration** (HTML Graphic only) — how long the stinger lasts. A clip's length
  comes from the file; a page has none, so it is declared here. The keyed input is hidden
  when it ends.

If the cut point plus the beneath duration would run past the end of the stinger, the
duration is shortened so the transition finishes while the graphic is still covering. You
are told both the duration declared and the one that was applied.

For a clip, the cut lands on the frame that carries the cut point, on both mixer backends.
A page does the same when its animation changes something visible on its first frame; see
[Timing a page](#timing-a-page).

## Firing one

Trigger a transition of type `stinger` on the vision mixer, naming the block that holds
the graphic. That is the whole request — the timing comes from the block. The
`duration_ms` field is ignored for a stinger.

A stinger owns the program bus until it ends. Firing another one on the same mixer while
one is running is refused rather than queued.

## Alpha

The graphic needs a real alpha channel — a keyed graphic, not a graphic on a black
background.

Alpha comes in two forms, and **the keyed input the graphic feeds must be declared with the
same one** in the Vision Mixer's **DSK Alpha** setting:

- **Straight** — colour is independent of alpha. What most exporters produce by default,
  and the default for both a clip and a DSK input.
- **Premultiplied** — colour has already been multiplied by alpha. Set **Clip Alpha** on
  the Media Player if the clip was exported this way. Web pages are always premultiplied.

The form cannot be detected from the video, so it has to be declared. A stinger whose source
and keyed input disagree is refused before anything goes on air, and a flow warns at start
about any graphic whose keyed input disagrees: composited the wrong way, a premultiplied
graphic comes out dark around every soft edge and a straight one on a premultiplied input
comes out too bright.

Clip formats that can carry alpha and are known to work:

| Format | Notes |
| --- | --- |
| FFV1 in Matroska | Lossless, intra-only, seeks cheaply. The safest choice. |
| VP8 or VP9 with alpha in WebM | The common web delivery format. |
| HEVC with alpha | macOS only. |

H.264 cannot carry an alpha channel at all, so an H.264 clip will not work as a stinger no
matter how it was exported.

## Writing a stinger page

A take changes the page's URL fragment (the part after `#`), which does not reload the
page. The page has to cooperate:

- **Start on `hashchange`.** Run the animation from the beginning each time the fragment
  changes. Strom owns the fragment, so do not use it for anything else.
- **Change something visible on the first frame.** See [Timing a page](#timing-a-page).
- **Stay transparent and still while idle.** The keyed input is hidden between takes, but a
  page that keeps animating while idle — a blinking cursor, a looping background — costs
  rendering time, and its next frame is taken as the start of the take, so the cut can be a
  frame out.
- **End transparent.** Clear everything by the end of **Stinger Duration**. The keyed input
  is hidden at that point either way, but a page still drawing is cut off mid-frame.
- **Load everything up front.** Fonts, images and videos the animation needs should be
  loaded when the page first opens, which is when the flow starts. Anything fetched at take
  time delays the start by however long it takes to arrive.
- **Use a transparent background**, and size the page to the block's **Resolution**.

### Timing a page

The cut point is measured from the start of the page's animation, and Strom finds that
start by watching for the first frame the page delivers after the take. Chromium only
delivers a frame when something on screen changes, so that works exactly when the animation
changes something visible straight away — any technique will do: CSS animations and
transitions, `requestAnimationFrame` moving elements, canvas or SVG.

A page whose animation begins invisibly delivers its first frame late: an element sliding
in from fully off the frame, a fade up from nothing, an animation with a start delay. Some
CSS animations also do this on the first take after the page loads, and not on later ones.
When no frame arrives within a few frames of the take, Strom times the cut from the take
itself instead. That keeps the cut within about a frame of the cut point, and logs a
warning naming the graphic:

```
HTML graphic stinger_page: no frame within 70 ms of the take, so the cut is timed from the
take and may be a frame out. A stinger page should change something visible on its first frame
```

For a cut that lands exactly on its frame every time, start the animation with something
already on screen — the leading edge of a wipe, a first visible step of a fade. A 1-pixel
element that changes on every animation frame also works for a design that has to open on
an empty frame; drawing on a canvas counts too, even when what is drawn is transparent.

A minimal page:

```html
<body style="margin:0;background:transparent;overflow:hidden">
  <div id="wipe" style="position:fixed;inset:0;background:#e10600;transform:translateX(-100%)"></div>
  <script>
    const wipe = document.getElementById('wipe');
    addEventListener('hashchange', () => {
      // Starts with a sliver on screen, so the first frame already shows it.
      wipe.animate(
        [{ transform: 'translateX(-96%)' }, { transform: 'translateX(0)', offset: 0.4 },
         { transform: 'translateX(0)', offset: 0.6 }, { transform: 'translateX(100%)' }],
        { duration: 1000, easing: 'ease-in-out' });
    });
  </script>
</body>
```

With **Stinger Duration** 1000 and **Stinger Cut Point** 500, the program changes while the
red panel fully covers the frame.

An alpha video inside the page (a `<video>` with a WebM that has alpha) also works, but its
first one or two frames can be skipped: the video's clock starts before its first frame is
decoded. A page drawn with CSS, canvas or SVG starts on its first frame.

HTML graphics need the gstcefsrc plugin, which the `strom-full` image provides. See
[HTML_RENDER.md](HTML_RENDER.md).

## Current limitations

- **No fill and key pairs.** A stinger clip is one file with an alpha channel. Clips
  delivered as separate fill and key files, or with a luma matte alongside, are not
  supported yet.
- **No audio from the graphic.** Any audio on the stinger is ignored and the program audio
  continues uninterrupted.
- **A changed clip is not ready immediately.** A declared source is held ready between
  takes, so a stinger normally starts the moment you fire it. Change that player's clip and
  fire straight away and the start can slip by close to a frame at 4K.
- **A page cannot say when it is ready.** Strom starts the page with the flow but cannot
  tell whether it has finished loading. Fire too soon after a flow starts and the graphic
  may be missing or late for that take.

## When the graphic will not play

If the clip is missing, unreadable or fails to decode, or the HTML Graphic is not running,
the transition beneath still runs on its own. You lose the branding for that take, but the
program source still changes — a broken graphic does not leave you stuck mid-transition. The
failure is reported so you can see which source was at fault.
