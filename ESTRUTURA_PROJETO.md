# Estrutura do Projeto Acoli - Visão Geral

## Tipo de Projeto
- **Framework**: Django 5 + DRF (Django REST Framework)
- **Frontend**: HTMX + Tailwind CSS (via CDN) + CSS customizado
- **Banco de Dados**: PostgreSQL (com suporte a SQLite localmente)
- **Template Engine**: Django Templates
- **Idioma**: Português Brasileiro
- **Localização**: C:\Users\fabio\Projects\acoli

---

## Estrutura de Diretórios

```
acoli/
├── acoli/                          # Configuração principal do Django
│   ├── settings.py                 # Configurações Django (DB, apps, middleware)
│   ├── urls.py                     # URLs principais do projeto
│   ├── wsgi.py                     # WSGI para deploy
│   └── asgi.py                     # ASGI para WebSockets
│
├── static/                         # Arquivos estáticos
│   └── css/
│       └── app.css                 # CSS customizado principal (496 linhas)
│
├── templates/                      # Templates Django
│   ├── base.html                   # Template base (297 linhas)
│   ├── dashboard.html              # Dashboard principal
│   ├── login.html                  # Login
│   ├── logout.html / logout_confirm.html
│   ├── acolytes/                   # Módulo de acolitos
│   │   ├── assignments.html        # Escala de acolitos
│   │   ├── swaps.html              # Sistema de trocas
│   │   ├── preferences.html        # Preferências
│   │   ├── swap_form.html
│   │   ├── link_user.html
│   │   ├── _partials/
│   │   │   ├── assignment_card.html
│   │   │   ├── swap_card.html
│   │   │   └── calendar_feed_section.html
│   ├── people/                     # Módulo de pessoas
│   │   ├── directory.html          # Diretório de pessoas
│   │   ├── acolyte_detail.html
│   │   ├── create.html
│   │   ├── detail.html
│   │   └── acolyte_tabs/          # Abas do detalhe de acolito
│   │       ├── overview.html
│   │       ├── schedule.html
│   │       ├── preferences.html
│   │       ├── availability.html
│   │       ├── qualifications.html
│   │       ├── notifications.html
│   │       ├── audit.html
│   │       ├── credits.html
│   │       ├── swaps.html
│   │       └── _assign_slot_modal.html
│   ├── calendar/                   # Módulo de calendário
│   │   ├── month.html              # Visualização mensal
│   │   ├── detail.html
│   │   ├── assign_slot.html
│   │   └── _slots_section.html
│   ├── events/                     # Módulo de eventos
│   │   ├── list.html
│   │   ├── form.html
│   │   ├── detail.html
│   │   ├── days.html
│   │   ├── basics.html
│   │   └── interest.html
│   ├── structure/                  # Estrutura paroquial
│   │   ├── communities_list.html / communities_form.html
│   │   ├── roles_list.html / roles_form.html
│   │   └── requirement_profiles_list.html / requirement_profiles_form.html
│   ├── scheduling/                 # Agendamento
│   │   ├── dashboard.html
│   │   ├── job_detail.html
│   │   └── publish_preview.html
│   ├── roster/                     # Escala geral
│   │   └── index.html
│   ├── replacements/               # Substituições
│   │   ├── center.html
│   │   ├── resolve.html
│   │   └── pick.html
│   ├── reports/                    # Relatórios
│   │   └── frequency.html
│   ├── audit/                      # Auditoria
│   │   └── list.html
│   ├── mass_templates/             # Modelos de missa
│   │   ├── list.html
│   │   └── form.html
│   ├── settings/                   # Configurações
│   │   └── parish.html
│   ├── partials/                   # Componentes compartilhados
│   │   └── _messages.html
│   ├── modals/                     # Modais
│   │   └── assignment_conflict.html
│
├── web/                            # App Django principal (web)
│   ├── views.py                    # Views/Controllers (178KB)
│   ├── forms.py                    # Formulários Django (44KB)
│   ├── urls.py                     # URLs da aplicação web
│   ├── templatetags/               # Tags customizadas do Django
│   ├── migrations/                 # Migrações do banco
│   ├── tests/                      # Testes
│   └── __init__.py
│
├── core/                           # App principal (modelos e lógica)
│   ├── models.py                   # Modelos do banco (23KB)
│   ├── admin.py                    # Admin Django
│   ├── middleware.py               # Middlewares
│   ├── context_processors.py       # Context processors
│   ├── migrations/                 # Migrações
│   ├── services/                   # Lógica de negócio
│   ├── tests/                      # Testes
│   ├── management/                 # Comandos customizados
│   └── __init__.py
│
├── accounts/                       # Módulo de autenticação
│   ├── migrations/
│   └── models.py
│
├── scheduler/                      # Módulo de agendamento (OR-Tools)
│   └── migrations/
│
├── notifications/                  # Módulo de notificações
│   └── migrations/
│
├── api/                            # API REST (DRF)
│
├── fixtures/                       # Dados de inicialização
│   └── seed_parish.json            # Dados iniciais
│
├── docs/                           # Documentação
│   ├── ARCHITECTURE_NOTES.md
│   └── USER_GUIDE.md
│
├── db.sqlite3                      # Banco de dados SQLite (local)
├── manage.py                       # CLI Django
├── requirements.txt                # Dependências Python
├── Procfile                        # Configuração Heroku
├── gunicorn.conf.py                # Configuração Gunicorn
├── scanner.py                      # Script utilitário
├── runtime.txt                     # Versão Python (Heroku)
└── .env.example                    # Exemplo de variáveis de ambiente
```

---

## CONFIGURAÇÃO DE ESTILOS

### 1. Sistema de Cores (Tema)
Definido em `templates/base.html` (linhas 10-29) usando Tailwind config:

```javascript
tailwind.config = {
  theme: {
    extend: {
      colors: {
        ink: "#0b1f26",        // Cor primária escura (preto/azul)
        slate: "#5a6a72",      // Cor secundária (cinza)
        sand: "#f3efe6",       // Fundo (bege claro)
        moss: "#0f5e4d",       // Cor destaque verde (botões, links)
        sun: "#d9a441",        // Cor accent (dourado/ouro)
      },
    },
  },
};
```

### 2. Paleta de Cores Utilizada

| Nome | Hex | Uso |
|------|-----|-----|
| **Ink** | #0b1f26 | Texto principal, elementos escuros |
| **Slate** | #5a6a72 | Texto secundário, placeholders |
| **Sand** | #f3efe6 | Fundo da página |
| **Moss** | #0f5e4d | Botões primários, links, highlights |
| **Sun** | #d9a441 | Badges, elementos de destaque |

### 3. Fontes
- **Sans-serif**: "Space Grotesk" (400, 500, 600, 700) - Headings e UI
- **Serif**: "Fraunces" (600, 700) - Títulos importantes
- Carregadas do Google Fonts

### 4. Arquivo CSS Customizado
**Localização**: `static/css/app.css` (496 linhas)

**Componentes Customizados**:
- `.nav-link` - Links de navegação
- `.card` - Cards com gradiente e sombra
- `.btn-primary` - Botão primário (moss verde)
- `.btn-secondary` - Botão secundário
- `.badge` / `.badge-*` - Badges (verde, vermelho, amarelo, azul, cinza)
- `.check-grid`, `.check-item`, `.check-row` - Checkboxes customizados
- `.custom-select` - Select customizado
- `.toggle-switch` / `.toggle-label` - Toggle switches
- `.form-error` / `.form-help` - Mensagens de formulário
- `.htmx-success-flash` - Animação de sucesso HTMX

### 5. Box Shadow Customizado
```css
boxShadow: {
  soft: "0 12px 30px rgba(10, 24, 32, 0.12)"  // Sombra suave padrão
}
```

### 6. Variações de Badges
- `.badge-green` - Verde (moss)
- `.badge-red` - Vermelho (#dc2626)
- `.badge-yellow` - Amarelo (sun)
- `.badge-blue` - Azul (#3b82f6)
- `.badge-gray` - Cinza (slate)

---

## LAYOUT PRINCIPAL

### Header (base.html - linhas 38-68)
- Logo "Acoli" (fonte serif, cor moss)
- Nome da paróquia ativa + cidade
- Botão de trocar paróquia (dropdown)
- Link "Sair" para logout
- Sticky no topo com backdrop blur

### Sidebar (Navegação - linhas 71-94)
- Visível apenas em desktop (lg:)
- Links de navegação principal
- Seção "Administração" (condicional)
- Seção "Estrutura" (condicional)
- Fundo com transparência + backdrop blur

### Mobile Footer Navig
