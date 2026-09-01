# FireSprites

A collection of animated SVG character sprites for web projects.

## Characters

| Character | Type | Colors | Animation |
|-----------|------|--------|-----------|
| Alpha | Robot | Green (#00e676) | Blinking, walking, pulsing |
| Beta | Robot | Blue (#448aff) | Screen display, walking |
| Gamma | Robot | Orange (#ff9100) | Radar scan, walking |

## How It Works

Each character is a self-contained SVG with CSS animations. No external assets needed.

### Building a Character

1. **Body** — Main shape (rect/circle) with stroke
2. **Eyes** — Animated circles/rects with blink animation
3. **Antenna** — Line + pulsing circle on top
4. **Arms** — Rectangles on sides
5. **Legs** — Rectangles at bottom
6. **Glow** — Semi-transparent circle behind body

### Animation Types

```css
/* Blinking eyes */
circle { animation: blink 4s infinite; }
@keyframes blink { 0%,90%,100%{r:2} 95%{r:0.5} }

/* Pulsing light */
circle { animation: pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

/* Walking bob */
.robot { animation: bob 0.6s infinite; }
@keyframes bob { 0%,100%{transform:translateY(0)} 50%{transform:translateY(-3px)} }
```

## Usage

Copy the SVG directly into your HTML:

```html
<div class="robot" style="left:50%;top:50%">
    <svg width="40" height="52" viewBox="0 0 40 52">
        <!-- Character SVG here -->
    </svg>
</div>
```

## License

Free to use in any project.
