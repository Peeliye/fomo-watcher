---
name: Algorithmic Terminal
colors:
  surface: '#10131a'
  surface-dim: '#10131a'
  surface-bright: '#363940'
  surface-container-lowest: '#0b0e14'
  surface-container-low: '#191c22'
  surface-container: '#1d2026'
  surface-container-high: '#272a31'
  surface-container-highest: '#32353c'
  on-surface: '#e1e2eb'
  on-surface-variant: '#bbcabf'
  inverse-surface: '#e1e2eb'
  inverse-on-surface: '#2e3037'
  outline: '#86948a'
  outline-variant: '#3c4a42'
  surface-tint: '#4edea3'
  primary: '#4edea3'
  on-primary: '#003824'
  primary-container: '#10b981'
  on-primary-container: '#00422b'
  inverse-primary: '#006c49'
  secondary: '#d3fbff'
  on-secondary: '#00363a'
  secondary-container: '#00eefc'
  on-secondary-container: '#00686f'
  tertiary: '#ffb2b7'
  on-tertiary: '#67001b'
  tertiary-container: '#ff7886'
  on-tertiary-container: '#780021'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#6ffbbe'
  primary-fixed-dim: '#4edea3'
  on-primary-fixed: '#002113'
  on-primary-fixed-variant: '#005236'
  secondary-fixed: '#7df4ff'
  secondary-fixed-dim: '#00dbe9'
  on-secondary-fixed: '#002022'
  on-secondary-fixed-variant: '#004f54'
  tertiary-fixed: '#ffdadb'
  tertiary-fixed-dim: '#ffb2b7'
  on-tertiary-fixed: '#40000d'
  on-tertiary-fixed-variant: '#92002a'
  background: '#10131a'
  on-background: '#e1e2eb'
  surface-variant: '#32353c'
  surface-bg: '#0B0E14'
  surface-card: '#121721'
  surface-elevated: '#181F2C'
  surface-overlay: '#06080B'
  border-grid: '#1B2230'
  border-active: '#2D3748'
  border-highlight: '#00F0FF'
  text-primary: '#FFFFFF'
  text-secondary: '#94A3B8'
  text-muted: '#475569'
  chain-solana: '#8B5CF6'
  chain-ethereum: '#3B82F6'
  chain-bnb: '#F59E0B'
  chain-base: '#06B6D4'
  status-online: '#10B981'
  status-warning: '#F59E0B'
  status-danger: '#F43F5E'
typography:
  headline-lg:
    fontFamily: Space Grotesk
    fontSize: 32px
    fontWeight: '700'
    lineHeight: 38px
  headline-lg-mobile:
    fontFamily: Space Grotesk
    fontSize: 24px
    fontWeight: '700'
    lineHeight: 30px
  headline-md:
    fontFamily: Space Grotesk
    fontSize: 20px
    fontWeight: '700'
    lineHeight: 26px
  headline-sm:
    fontFamily: Space Grotesk
    fontSize: 16px
    fontWeight: '600'
    lineHeight: 22px
  body-lg:
    fontFamily: Space Mono
    fontSize: 14px
    fontWeight: '400'
    lineHeight: 20px
  body-md:
    fontFamily: Space Mono
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 18px
  body-sm:
    fontFamily: Space Mono
    fontSize: 11px
    fontWeight: '400'
    lineHeight: 16px
  label-lg:
    fontFamily: Space Mono
    fontSize: 12px
    fontWeight: '700'
    lineHeight: 16px
  label-md:
    fontFamily: Space Mono
    fontSize: 10px
    fontWeight: '700'
    lineHeight: 14px
  label-sm:
    fontFamily: Space Mono
    fontSize: 9px
    fontWeight: '700'
    lineHeight: 12px
  data-metric:
    fontFamily: Space Grotesk
    fontSize: 22px
    fontWeight: '700'
    lineHeight: 26px
  data-mono-num:
    fontFamily: Space Mono
    fontSize: 12px
    fontWeight: '700'
    lineHeight: 16px
spacing:
  gutter: 0.5rem
  gutter-mobile: 0.25rem
  margin: 0.75rem
  margin-mobile: 0.5rem
  space-xs: 0.125rem
  space-sm: 0.25rem
  space-md: 0.5rem
  space-lg: 0.75rem
  space-xl: 1rem
---

## Brand & Style

The design system targets high-frequency crypto traders, on-chain algorithmic strategists, and KOL copy-trading power users who operate in sub-second execution environments. The personality is uncompromisingly brutalist, technical, low-latency, and analytical. It explicitly avoids consumer-grade "fintech fluff", friendly rounded corners, unnecessary gradients, and decorative animations. 

The aesthetic is grounded in retro-futuristic terminal command centers and cybernetic telemetry monitors. It balances relentless density with laser-sharp legibility: 0px border radiuses, razor-thin structural framing, monospaced tabular data grids, high-contrast monochrome containers, and hyper-saturated signal status nodes (terminal cyan, profit emerald, drawdown ruby, and multi-chain protocol identifiers). The emotional response evoked is total systemic control, clinical precision, speed, and institutional confidence under extreme market volatility.

## Colors

The color palette is built around an ink-black telemetry base (`#0B0E14`) paired with precision architectural borders (`#1B2230`) and dark container cells (`#121721`). Surfaces remain deliberately subdued so actionable signals and financial deltas dominate user attention.

- **Primary (`#10B981`):** Serves as positive alpha, executed buys, confirmed state indicators, WebSocket sync indicators, and high-frequency fill verification.
- **Secondary (`#00F0FF`):** Cybernetic telemetry accent representing algorithmic actions, trigger routes, active contract targets, copy-bot configurations, and interactive terminal prompts.
- **Tertiary (`#F43F5E`):** Hard risk limits, negative PnL drawdown, slippage breaches, unhedged positions, and halt triggers.
- **Neutral (`#0B0E14`):** Near-black deep slate substrate that simulates CRT algorithmic consoles and OLED power efficiency.
- **Multi-Chain Signatures:** Dedicated unshifted network hex codes (`Solana #8B5CF6`, `Ethereum #3B82F6`, `BNB #F59E0B`, `Base #06B6D4`) provide instant protocol disambiguation within crowded order books.

## Typography

Typography prioritizes fixed-width alignment and instantaneous data scanning. 

`Space Grotesk` drives high-level telemetry headers and primary financial metrics, delivering an authoritative technological punch without sacrificing numeric legibility. 

`Space Mono` governs all data tables, contract addresses, token quantities, latency timers, and transactional labels. All monetary values, transaction hashes, timestamps, and percentages must enable font-feature-settings `tnum` (tabular numbers) and `zero` (slashed zero) to prevent layout jitter during rapid real-time state mutation.

Text sizes are kept compact and dense to maximize viewport throughput on mobile screens. Monospaced labels use uppercase micro-typography (`label-sm` at 9px/10px) with slight letter tracking (`+0.05em`) for diagnostic readouts.

## Layout & Spacing

The layout philosophy follows a high-density, modular terminal matrix. There is zero wasted negative space; layouts conform to an edge-to-edge algorithmic HUD structure.

- **Grid Model:** Mobile displays deploy a rigid 4-column micro-grid with `0.25rem` (4px) gutters, allowing split-metric blocks (e.g. 2-column or 4-column telemetry monitors). Tablet and desktop expansions scale to 8 and 12 columns respectively using an 8px (`0.5rem`) gutter.
- **Rhythm & Padding:** Component spacing is calibrated to tight 2px and 4px multiples (`0.125rem` to `0.5rem`). Container padding is minimal to maximize visible order rows and chart viewport area.
- **Breakpoint Rules:** 
  - Mobile (`< 640px`): Outer margins are pinned to `0.5rem`. Complex data rows fold into segmented tabular cards featuring stacked dual-key values (e.g. Token/Chain top, KOL/Timestamp bottom).
  - Desktop / Tablet (`>= 640px`): Outer margins scale to `0.75rem` with full uncollapsed tabular sheets, persistent global stat-bars, and multi-pane order-book docks.

## Elevation & Depth

Visual depth is achieved strictly through **low-contrast architectural outlines** and **tonal planar stepping**. Diffused Gaussian drop shadows and blurred skeuomorphic overlays are prohibited.

- **Grid Containment:** All panels, metrics blocks, and tables are defined by sharp 1px solid borders (`#1B2230`).
- **Layer Stacking:**
  - `Level 0 (Canvas Base)`: Pure `#0B0E14` void.
  - `Level 1 (Data Grid Cards)`: `#121721` planar surface bounded by 1px `#1B2230`.
  - `Level 2 (Active/Hover/Pressed Cells)`: `#181F2C` highlight surface with `#2D3748` border.
  - `Level 3 (Modal Execution Sheets / Terminal Prompts)`: `#06080B` with an intense 1px `#00F0FF` cyan or `#10B981` emerald telemetry frame.
- **Telemetry Glow Accents:** Real-time state indicators (such as live WebSocket pulses and instant copy execution nodes) utilize a razor-sharp 1px border or a single high-intensity inner stroke rather than soft outer glows, maintaining algorithmic terminal clarity.

## Shapes

The design system is strictly **Sharp (`roundedness: 0`)**. Every component, modal, badge, button, and input card possesses a hard `0px` border radius. 

Corners reflect pure industrial geometry. Angled chamfers (e.g., 45-degree clipped corners via CSS `clip-path`) are permitted strictly for terminal control tabs, contract action chips, or system status flags to reinforce the cybernetic defense-terminal visual motif. Dividers and structural partitions are constructed using continuous 1px orthoganal lines.

## Components

### Buttons
- **Primary Terminal Action (Execute / Copy / Buy):** Solid `#10B981` background, `#0B0E14` Space Grotesk Bold text, 0px border radius, 36px mobile height. Active state inverts to `#00F0FF` background.
- **Secondary / Utility Action (Copy CA / RPC Switch):** Transparent background, 1px solid `#1B2230` border, `#FFFFFF` Space Mono text. Hover/pressed states switch border to `#00F0FF` with a subtle `#181F2C` fill.
- **Emergency / Revoke Action:** Solid `#F43F5E` background or 1px `#F43F5E` border with crimson monospace text.

### Chips & Protocol Badges
- **Chain Badges:** Solid dark container (`#121721`) with a 1px border matching the native chain hex (Solana `#8B5CF6`, Ethereum `#3B82F6`, BNB `#F59E0B`, Base `#06B6D4`). Font is `label-sm` uppercase.
- **Status Badges:** Compact pill-free rectangles (`0px` radius, padding: 2px 6px). Live status features a solid 4px square indicator dot (e.g., Emerald green `#10B981` for active WebSocket).

### Data Tables & Rows
- Alternating rows employ 1px bottom borders (`#1B2230`).
- Columns align strictly: Text left, financial numbers and PnL metrics right.
- KOL identifiers are rendered in `#FFFFFF` with `@` in `#00F0FF`.
- Token symbols render in `Space Grotesk Bold`, accompanied by the chain identifier in a micro muted monospace subtitle.

### Input Fields & Sliders
- Dark `#06080B` background with 1px `#1B2230` outline. Focus transitions border to `#00F0FF` with no outline blur.
- Numeric inputs (Slippage, Gas Priority, Tip Amount) feature embedded quick-preset increments (`+0.1`, `+0.5`, `MAX`) as monospaced sharp buttons inside the input box frame.

### Metric Cards (Telemetry Blocks)
- Modular square-edged cells surrounded by 1px `#1B2230`.
- Top row: Muted uppercase diagnostic label (`label-md`).
- Bottom row: Primary metric (`data-metric`) accompanied by absolute and percentage delta chips highlighted in `#10B981` (profit) or `#F43F5E` (loss).