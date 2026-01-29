# Design System
## Direction
Personality: Precision & Density
Depth: Borders-only
Foundation: Cool slate

## Tokens
Spacing base: 4px
Scale: 4,8,12,16,24,32
Radius: 8px
Button height: 36px
Card padding: 16px

## Patterns
Button Primary: gradiente moss (linear-gradient 135deg #0f5e4d to #0a4a3d), color white, pad 0.75rem 1.25rem, radius 0.875rem, font 0.9rem weight 500, shadow rgba(15,94,77,0.2), hover translateY(-2px) e shadow maior
Button Secondary: gradiente sutil ink, color ink, pad 0.625rem 1rem, radius 0.875rem, font 0.85rem weight 500, border rgba(10,24,32,0.08), hover translateY(-1px)
Button Danger: similar a primary mas vermelho (linear-gradient 135deg #dc2626 to #b91c1c), color white, pad 0.75rem 1.25rem, radius 0.875rem, font 0.9rem weight 500, shadow rgba(220,38,38,0.2), hover translateY(-2px) e shadow maior
Card Default: border 0.5px, pad 16px, radius 10px
Assignment Card: bg-white, border-sand-200, radius=12px, pad=20px, hover:shadow-soft, icons for time/community/date, status badges with colors (moss for confirmed, sun for pending, slate for proposed), primary buttons use btn-primary, secondary use btn-secondary, danger use btn-danger
People Directory: simple header with btn-primary for new, real-time search bar with internal icon and HTMX, collapsible advanced filters with toggle button, filters update on change, cards with sand avatar bg, reliability bar or n/d text, simple empty state