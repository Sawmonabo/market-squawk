'use strict';

const PROVIDER_COPY = Object.freeze({
  'coinbase.public-market-data': {
    mark: 'CB',
    name: 'Coinbase',
    purpose: 'Live cryptocurrency trades and order books from a direct public market feed.',
    examples: ['Bitcoin and Ether trades', 'Best bids and offers', 'Price-level order books'],
    goals: ['live'],
    effort: 'Automatic',
    access: 'No account',
    account: 'Market data is public. No Coinbase login or API key is needed.'
  },
  'coinbase.exchange-direct-market-data': {
    mark: 'CB+',
    name: 'Coinbase Exchange',
    purpose: 'Direct Coinbase Exchange market data with provider-issued credentials.',
    examples: ['Authenticated exchange feeds', 'Direct venue coverage', 'Provider account streams'],
    goals: [],
    effort: 'About 3 minutes',
    access: 'API key required',
    account:
      'Use one Coinbase Exchange portfolio and a View-only key. Coinbase requires an IP-address allowlist and two-step verification when the key is created.',
    handoffUrl:
      'https://help.coinbase.com/en/exchange/managing-my-account/how-to-create-an-api-key',
    handoffInstruction:
      'Create a Coinbase Exchange key for one portfolio with View permission only. Leave Trade, Transfer, and Manage off, add the required IP-address allowlist, complete two-step verification, and save the passphrase and API secret when Coinbase shows them once.',
    setupSteps: [
      'Open the official Coinbase Exchange API-key instructions.',
      'Choose one portfolio and create a key with View permission only.',
      'Add the required IP-address allowlist and complete two-step verification.',
      'Save the public API key, passphrase, and API secret when Coinbase displays them.',
      'Return here and submit those three values once.'
    ],
    submitLabel: 'Save Coinbase key and activate',
    renewal: {
      manageLabel: 'Rotate API key',
      title: 'Rotate the Coinbase Exchange API key',
      description:
        'Coinbase’s published rotation steps revoke the old key before the new key is created, so the direct feed may pause. Create the replacement with View permission only and the required IP-address allowlist, then return with all three credential values.',
      handoffUrl:
        'https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key',
      continueLabel: 'I have the replacement key',
      submitLabel: 'Save replacement and reconnect'
    }
  },
  'alpaca.basic-market-data': {
    mark: 'AL',
    name: 'Alpaca Basic',
    purpose:
      'No-monthly-fee U.S. equity and ETF market data from Alpaca’s IEX feed, using your own Alpaca account credentials.',
    examples: [
      'Real-time IEX stock and ETF quotes',
      'Up to 30 streamed symbols on the Basic plan',
      'Delayed IEX historical market data'
    ],
    goals: ['live'],
    effort: 'About 3 minutes',
    access: 'API key required',
    account:
      'Alpaca’s Basic market-data option has no monthly data fee, but Alpaca still requires an account and Trading API key pair. Paper Trading is the recommended V1 realm and does not place real orders.',
    handoffUrl: 'https://app.alpaca.markets/signup',
    handoffInstruction:
      'Create or sign in to Alpaca, open Paper Trading, generate an API key pair, and save the key ID and secret when Alpaca shows them. The secret is shown once. Return here instead of pasting either value into chat.',
    setupSteps: [
      'Open the official Alpaca dashboard and create or sign in to your account.',
      'Choose Paper Trading for the recommended V1 setup.',
      'Open API Keys and generate a new key pair.',
      'Save the API key ID and secret key when Alpaca displays them; the secret is shown once.',
      'Return here, choose the same Paper or Live realm, and submit both values once.'
    ],
    submitLabel: 'Save Alpaca key pair and activate',
    renewal: {
      manageLabel: 'Rotate API key pair',
      title: 'Rotate the Alpaca API key pair',
      description:
        'Generate a replacement key pair in the same Alpaca Paper or Live realm, save the new secret when it is shown, and return with both replacement values.',
      handoffUrl: 'https://app.alpaca.markets/',
      continueLabel: 'I have the replacement key pair',
      submitLabel: 'Save replacement and reconnect'
    }
  },
  'kraken.spot-public-market-data': {
    mark: 'KR',
    name: 'Kraken',
    purpose: 'A second live cryptocurrency market for comparison and resilience.',
    examples: ['Spot order books', 'Cross-market price comparison', 'Checksum-verified updates'],
    goals: ['live'],
    effort: 'Automatic',
    access: 'No account',
    account: 'Kraken provides this public market feed without an account or API key.'
  },
  'sec.edgar-public': {
    mark: 'SEC',
    name: 'SEC EDGAR',
    purpose: 'Company filings, financial statements, and reported facts.',
    examples: ['10-K and 10-Q filings', 'XBRL company facts', 'Filed financial statements'],
    goals: ['companies'],
    effort: 'About 1 minute',
    access: 'Contact details',
    account:
      'No SEC account or API key is needed. Enter a truthful organization or application name and a monitored administrative email for the declared User-Agent.',
    handoffUrl:
      'https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data',
    showOfficialLink: true,
    setupSteps: [
      'Review SEC’s official fair-access and declared User-Agent guidance.',
      'Find the company by name or ticker on SEC.gov and copy its 10-digit CIK.',
      'Enter a truthful organization or application name and administrative contact email below.',
      'Market Squawk sends that non-secret contact in its declared User-Agent.',
      'The verified SEC configuration is saved locally; no account or API key is created.'
    ],
    contact: {
      legend: 'SEC declared contact',
      hint:
        'SEC requires a truthful organization or application name and a monitored administrative email in automated request headers.',
      organizationLabel: 'Organization or application name',
      emailLabel: 'Administrative contact email'
    }
  },
  'fred-alfred.api-v1-v2': {
    mark: 'FR',
    name: 'FRED and ALFRED',
    purpose: 'Economic indicators and the historical revisions available at each point in time.',
    examples: ['Unemployment and inflation', 'Historical data vintages', 'Economic research inputs'],
    goals: [],
    effort: 'Permission review',
    access: 'Written permission + free API key',
    account:
      'A free FRED account and API key provide access. Saving data or using it for model training also requires an exact written St. Louis Fed permission response and separate authority for each selected series.',
    handoffUrl: 'https://fred.stlouisfed.org/docs/api/api_key.html',
    handoffInstruction:
      'After importing and reviewing the written permission response below, create or sign in to a free FRED account and request a distinct Market Squawk API key.',
    setupSteps: [
      'If you only need unemployment data, use the recommended public BLS source instead.',
      'Request written St. Louis Fed permission for Market Squawk’s exact FRED API use.',
      'Import the exact response and record your local scope review below.',
      'Create or sign in to a free FRED account and request a distinct API key.',
      'Return here and submit the API key once.'
    ],
    credentialLabel: 'FRED API key',
    submitLabel: 'Save FRED API key and activate',
    renewal: {
      manageLabel: 'Replace API key',
      title: 'Replace the FRED API key',
      description:
        'FRED’s public documentation does not define routine key expiration or a standard rotation schedule. Use the authenticated FRED key page and continue only if FRED has issued a different key.',
      continueLabel: 'I have a different FRED key',
      submitLabel: 'Save new FRED key and activate'
    }
  },
  'bls.v1-unregistered': {
    mark: 'BLS',
    name: 'Bureau of Labor Statistics',
    purpose: 'Employment, inflation, pay, and labor-market statistics.',
    examples: ['U.S. unemployment rate', 'Consumer prices', 'Employment and earnings'],
    goals: ['economy'],
    effort: 'Automatic',
    access: 'No account',
    account: 'The public BLS v1 interface needs no registration or API key.'
  },
  'bls.v2-registered': {
    mark: 'BLS+',
    name: 'BLS registered access',
    purpose: 'Registered BLS access for larger, provider-authorized requests.',
    examples: ['Larger series batches', 'Longer request windows', 'Registered-tier access'],
    goals: [],
    effort: 'About 3 minutes',
    access: 'Free API key',
    account:
      'Register with an organization name and email, complete the provider CAPTCHA, accept the BLS terms, and retrieve the registration key emailed by BLS.',
    handoffUrl: 'https://data.bls.gov/registrationEngine/',
    handoffInstruction:
      'Enter your organization name and email on the official BLS page, complete the image CAPTCHA, accept the BLS terms, and retrieve the registration key emailed by labstat@bls.gov.',
    setupSteps: [
      'Open the official BLS Public Data API registration page.',
      'Enter your organization name and email address.',
      'Complete the image CAPTCHA and accept the BLS terms.',
      'Retrieve the registration key emailed by labstat@bls.gov.',
      'Return here and submit that registration key once.'
    ],
    credentialLabel: 'BLS registration key',
    submitLabel: 'Save BLS registration key and activate',
    contact: {
      legend: 'BLS registration contact',
      hint:
        'Use the same truthful organization and monitored email that you provide on the official BLS registration page.',
      organizationLabel: 'Organization name',
      emailLabel: 'Registration email'
    },
    renewal: {
      manageLabel: 'Review annual renewal',
      title: 'Complete the annual BLS registration renewal',
      description:
        'BLS requires registration renewal at least once a year, but its public FAQ does not say whether the registration key changes. Complete the official renewal step, then enter the key BLS tells you to use.',
      continueLabel: 'Continue to BLS key verification',
      submitLabel: 'Verify renewed BLS registration'
    }
  },
  'treasury.daily-rates-xml': {
    mark: 'UST',
    name: 'U.S. Treasury rates',
    purpose: 'Official interest rates and yield curves published by the U.S. Treasury.',
    examples: ['Treasury yield curves', 'Bill rates', 'Real and long-term rates'],
    goals: ['economy'],
    effort: 'Automatic',
    access: 'No account',
    account: 'The official Treasury XML feeds need no account or API key.'
  },
  'treasury.fiscal-data': {
    mark: 'FD',
    name: 'Treasury Fiscal Data',
    purpose: 'Federal financial datasets from the official Fiscal Data API.',
    examples: ['Average interest rates', 'Federal financial series', 'Dataset-version provenance'],
    goals: ['economy'],
    effort: 'Automatic',
    access: 'No account',
    account: 'The Fiscal Data API is public and needs no account or API key.'
  },
  'local.files': {
    mark: 'FILE',
    name: 'Your local files',
    purpose: 'Use CSV, JSON, NDJSON, Parquet, and other files you already own.',
    examples: ['Historical prices', 'Research exports', 'Licensed or personal datasets'],
    goals: ['portfolio'],
    effort: 'When needed',
    access: 'Local only',
    account: 'No online account or key is needed. You choose each file from the local CLI.'
  },
  'local.portfolio-imports': {
    mark: 'PORT',
    name: 'Portfolio imports',
    purpose: 'Bring in holdings and transactions for local portfolio research.',
    examples: ['Holdings', 'Transactions and cost basis', 'Cash and account records'],
    goals: ['portfolio'],
    effort: 'When needed',
    access: 'Local only',
    account: 'No online account is needed. Import a broker export or local portfolio file.'
  },
  'local.paper-execution': {
    mark: 'PPR',
    name: 'Paper execution',
    purpose: 'Practice strategies locally without submitting live-money orders.',
    examples: ['Risk-approved paper orders', 'Simulated fills', 'Positions and balances'],
    goals: ['portfolio'],
    effort: 'Automatic',
    access: 'Local only',
    account: 'No broker account or API key is needed for local paper execution.'
  }
});

const GOALS = Object.freeze([
  {
    id: 'live',
    mark: 'LIVE',
    title: 'Live markets',
    description: 'Watch live cryptocurrency trades, prices, and order books.'
  },
  {
    id: 'economy',
    mark: 'ECO',
    title: 'Economic data and interest rates',
    description: 'Research jobs, inflation, economic history, and Treasury rates.'
  },
  {
    id: 'companies',
    mark: 'CO',
    title: 'Company filings and fundamentals',
    description: 'Read company filings, reported facts, and financial statements.'
  },
  {
    id: 'portfolio',
    mark: 'YOU',
    title: 'Portfolio research',
    description: 'Analyze your own files, holdings, transactions, and paper strategies.'
  },
  {
    id: 'all',
    mark: 'ALL',
    title: 'Everything recommended',
    description: 'Set up the complete zero-fee local starter collection.'
  }
]);

const RECOMMENDED_PROVIDERS = Object.freeze([
  'coinbase.public-market-data',
  'kraken.spot-public-market-data',
  'sec.edgar-public',
  'bls.v1-unregistered',
  'treasury.daily-rates-xml',
  'treasury.fiscal-data',
  'local.files',
  'local.portfolio-imports',
  'local.paper-execution'
]);

const GOAL_PROVIDERS = Object.freeze({
  live: ['coinbase.public-market-data', 'kraken.spot-public-market-data'],
  economy: [
    'bls.v1-unregistered',
    'treasury.daily-rates-xml',
    'treasury.fiscal-data'
  ],
  companies: ['sec.edgar-public'],
  portfolio: ['local.files', 'local.portfolio-imports', 'local.paper-execution']
});

const LOCAL_GUIDANCE = Object.freeze({
  'local.files': 'market-squawk ingest file --help',
  'local.portfolio-imports': 'market-squawk portfolio import --help',
  'local.paper-execution': 'market-squawk bot start --help'
});

const ERROR_COPY = Object.freeze({
  invalid_request: [
    'That request could not be accepted',
    'Check the fields shown on this page and try again. Your completed setup work was preserved.'
  ],
  forbidden: [
    'This setup session is no longer valid',
    'Reload the local setup page to receive a fresh protected session. No credential was displayed.'
  ],
  body_too_large: [
    'That value is too large',
    'Use the bounded provider value requested by the field and try again.'
  ],
  invalid_session_state: [
    'This provider changed state',
    'Your saved provider session was preserved. Reload it and continue from the current step.'
  ],
  provider_rate_limited: [
    'The provider asked us to slow down',
    'Wait briefly, then continue this saved setup from the same provider step.'
  ],
  provider_deadline_elapsed: [
    'The provider took too long to respond',
    'Your progress was preserved. Check your connection and safely try the step again.'
  ],
  invalid_unlock: [
    'The local secret store did not unlock',
    'Check the unlock phrase and try again. The phrase was not retained on this page.'
  ],
  fallback_unavailable: [
    'The encrypted local secret store is unavailable',
    'Use the operating-system credential store or restart setup after repairing the local fallback.'
  ],
  operation_cancelled: [
    'The operation was cancelled',
    'No incomplete authority was granted. You can safely continue from the saved provider state.'
  ],
  invalid_adapter_request: [
    'The provider settings were not accepted',
    'Review the date range, series details, and evidence fields, then try again.'
  ],
  adapter_activation_unavailable: [
    'This provider cannot be activated yet',
    'The required provider capability is not currently admitted. The setup remains visibly incomplete.'
  ],
  adapter_state_unavailable: [
    'The local provider state could not be saved',
    'Nothing was silently activated. Check local storage and try again.'
  ],
  operation_unavailable: [
    'This action is not available right now',
    'Reload the provider state and follow the next action shown.'
  ],
  not_found: [
    'The setup action was not found',
    'Reload this local page and try again from the current provider.'
  ]
});

const state = {
  csrf: '',
  profiles: [],
  sessions: new Map(),
  providerDatasets: new Map(),
  fallback: 'disabled',
  route: 'welcome',
  goals: new Set(),
  plan: [],
  activeIndex: 0,
  busy: false,
  notice: null,
  technical: null,
  pendingRequests: new Map(),
  advancedFilter: '',
  providerMode: 'guided',
  renewingProfile: null,
  focusHeading: true
};

const headerRoot = document.getElementById('app-header');
const mainRoot = document.getElementById('app-main');
const announcer = document.getElementById('announcer');

class PortalError extends Error {
  constructor(code, status, detail) {
    super(code);
    this.name = 'PortalError';
    this.code = code;
    this.status = status;
    this.detail = detail;
  }
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function actionButton(label, className, handler) {
  const node = element('button', `button ${className || ''}`.trim(), label);
  node.type = 'button';
  node.disabled = state.busy;
  node.addEventListener('click', handler);
  return node;
}

function externalLink(label, href) {
  const node = element('a', 'button', label);
  node.href = href;
  node.target = '_blank';
  node.rel = 'noopener noreferrer';
  return node;
}

function announce(message) {
  announcer.textContent = '';
  window.requestAnimationFrame(() => {
    announcer.textContent = message;
  });
}

function routeTo(route) {
  state.route = route;
  state.notice = null;
  state.focusHeading = true;
  render();
  announce(routeAnnouncement(route));
}

function routeAnnouncement(route) {
  const announcements = {
    welcome: 'Welcome to provider setup.',
    goals: 'Choose what you want to do.',
    review: 'Review your recommended setup plan.',
    provider: 'Provider setup step loaded.',
    completion: 'Setup summary loaded.',
    advanced: 'Advanced provider setup loaded.'
  };
  return announcements[route] || 'Setup page updated.';
}

function render() {
  renderHeader();
  mainRoot.replaceChildren();
  let content;
  if (state.route === 'goals') {
    content = renderGoalSelection();
  } else if (state.route === 'review') {
    content = renderPlanReview();
  } else if (state.route === 'provider') {
    content = renderProviderStep(state.plan[state.activeIndex]);
  } else if (state.route === 'completion') {
    content = renderCompletion();
  } else if (state.route === 'advanced') {
    content = renderAdvanced();
  } else {
    content = renderWelcome();
  }
  mainRoot.append(content);
  const shouldFocusHeading = state.focusHeading;
  state.focusHeading = false;
  if (shouldFocusHeading) {
    window.requestAnimationFrame(() => {
      const heading = mainRoot.querySelector('[data-page-heading]');
      if (heading) {
        heading.setAttribute('tabindex', '-1');
        heading.focus({preventScroll: true});
      }
    });
  }
}

function renderHeader() {
  headerRoot.replaceChildren();
  const brand = element('button', 'brand');
  brand.type = 'button';
  brand.setAttribute('aria-label', 'Market Squawk provider setup home');
  brand.addEventListener('click', () => routeTo('welcome'));
  const mark = element('span', 'brand-mark', 'MS');
  mark.setAttribute('aria-hidden', 'true');
  const copy = element('span', 'brand-copy');
  copy.append(
    element('span', 'brand-name', 'Market Squawk'),
    element('span', 'brand-context', 'Data source setup')
  );
  brand.append(mark, copy);

  const actions = element('div', 'header-actions');
  actions.append(element('span', 'local-pill', 'Runs locally'));
  const advanced = element(
    'button',
    'text-button',
    state.route === 'advanced' ? 'Guided setup' : 'Advanced'
  );
  advanced.type = 'button';
  advanced.addEventListener('click', () => {
    routeTo(state.route === 'advanced' ? guidedReturnRoute() : 'advanced');
  });
  actions.append(advanced);
  headerRoot.append(brand, actions);
}

function guidedReturnRoute() {
  if (state.plan.length === 0) return 'welcome';
  if (state.activeIndex < state.plan.length) return 'provider';
  return 'completion';
}

function renderWelcome() {
  const root = element('section', 'hero');
  const eyebrow = element('p', 'eyebrow', 'Secure, self-hosted setup');
  const heading = element('h1', '', 'Connect your free data sources');
  heading.dataset.pageHeading = '';
  const copy = element(
    'p',
    'hero-copy',
    'Choose what you want to explore. Market Squawk will recommend the right sources, explain each step, and keep your configuration on this computer.'
  );
  const actions = element('div', 'button-row');
  actions.append(
    actionButton('Set up recommended sources', 'button-primary', () => {
      state.goals = new Set(['all']);
      state.plan = buildPlan();
      state.activeIndex = 0;
      state.providerMode = 'guided';
      routeTo('goals');
    }),
    actionButton('Choose sources myself', '', () => {
      state.goals.clear();
      state.plan = [];
      state.activeIndex = 0;
      state.providerMode = 'guided';
      routeTo('goals');
    })
  );
  const privacy = element('aside', 'privacy-card');
  const privacyMark = element('span', 'privacy-mark', '✓');
  privacyMark.setAttribute('aria-hidden', 'true');
  const privacyCopy = element('div');
  privacyCopy.append(
    element('strong', '', 'Your credentials stay local'),
    element(
      'p',
      '',
      'Keys are sent only to the Market Squawk process running on this computer, stored through the local secret store, and never shown again.'
    )
  );
  privacy.append(privacyMark, privacyCopy);
  root.append(eyebrow, heading, copy, actions, privacy);
  if (state.notice) root.prepend(renderNotice());
  return root;
}

function renderWizardFrame(step, content) {
  const workspace = element('div', 'workspace');
  const rail = element('aside', 'progress-rail');
  rail.setAttribute('aria-label', 'Setup progress');
  rail.append(element('div', 'progress-label', 'Your setup'));
  const list = element('ol', 'progress-list');
  ['Choose goals', 'Review plan', 'Connect sources', 'Ready'].forEach((label, index) => {
    const item = element(
      'li',
      `progress-item${index === step ? ' is-current' : ''}${index < step ? ' is-complete' : ''}`
    );
    item.append(
      element('span', 'progress-number', index < step ? '✓' : String(index + 1)),
      element('span', '', label)
    );
    if (index === step) item.setAttribute('aria-current', 'step');
    list.append(item);
  });
  rail.append(list);
  const body = element('div', 'workspace-content');
  if (state.notice) body.append(renderNotice());
  body.append(content);
  workspace.append(rail, body);
  return workspace;
}

function renderGoalSelection() {
  const content = element('section');
  const header = element('header', 'page-header');
  header.append(
    element('p', 'eyebrow', 'Step 1 of 4'),
    pageHeading('What would you like to do?'),
    element(
      'p',
      'page-copy',
      'No finance knowledge is needed. Pick one or more goals and we will choose the useful free sources.'
    )
  );
  const grid = element('div', 'goal-grid');
  for (const goal of GOALS) {
    const label = element('label', 'goal-card');
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.name = 'goal';
    checkbox.value = goal.id;
    checkbox.checked = state.goals.has(goal.id);
    checkbox.addEventListener('change', () => updateGoal(goal.id, checkbox.checked));
    const mark = element('span', 'goal-mark', goal.mark);
    mark.setAttribute('aria-hidden', 'true');
    const copy = element('span', 'goal-copy');
    copy.append(element('h3', '', goal.title), element('p', '', goal.description));
    const check = element('span', 'goal-check', '✓');
    check.setAttribute('aria-hidden', 'true');
    label.append(checkbox, mark, copy, check);
    grid.append(label);
  }
  const actions = element('div', 'selection-actions');
  actions.append(
    actionButton('Back', 'button-quiet', () => routeTo('welcome')),
    actionButton('Review my plan', 'button-primary', () => {
      state.plan = buildPlan();
      if (state.plan.length === 0) {
        showNotice(
          'warning',
          'Choose at least one goal',
          'Select what you want to explore so Market Squawk can build a setup plan.'
        );
        return;
      }
      state.activeIndex = 0;
      state.providerMode = 'guided';
      routeTo('review');
    })
  );
  actions.lastElementChild.disabled = state.busy || state.goals.size === 0;
  content.append(header, grid, actions);
  return renderWizardFrame(0, content);
}

function updateGoal(goal, checked) {
  if (goal === 'all' && checked) {
    state.goals = new Set(['all']);
  } else {
    state.goals.delete('all');
    if (checked) {
      state.goals.add(goal);
    } else {
      state.goals.delete(goal);
    }
  }
  render();
}

function buildPlan() {
  const requested = [];
  if (state.goals.has('all')) {
    requested.push(...RECOMMENDED_PROVIDERS);
  } else {
    for (const goal of state.goals) {
      requested.push(...(GOAL_PROVIDERS[goal] || []));
    }
  }
  const unique = [...new Set(requested)];
  const profiles = new Map(state.profiles.map((profile) => [profile.id, profile]));
  return unique.map((id) => profiles.get(id)).filter(Boolean);
}

function renderPlanReview() {
  const content = element('section');
  const header = element('header', 'page-header');
  header.append(
    element('p', 'eyebrow', 'Step 2 of 4'),
    pageHeading('Here is your setup plan'),
    element(
      'p',
      'page-copy',
      'We will guide you through one source at a time. You can skip any source and return later.'
    )
  );
  const list = element('ol', 'plan-list');
  for (const profile of state.plan) {
    const copy = providerCopy(profile);
    const item = element('li', 'plan-card');
    const mark = element('span', 'provider-mark', copy.mark);
    mark.setAttribute('aria-hidden', 'true');
    const body = element('div', 'provider-copy');
    body.append(element('h3', '', copy.name), element('p', '', copy.purpose));
    const meta = element('div', 'plan-meta');
    meta.append(
      badge(copy.access, accessBadgeClass(copy.access)),
      badge(copy.effort, ''),
      statusBadge(profile)
    );
    body.append(meta);
    item.append(mark, body, element('span', 'effort', copy.effort));
    list.append(item);
  }
  const actions = element('div', 'selection-actions');
  actions.append(
    actionButton('Change goals', 'button-quiet', () => routeTo('goals')),
    actionButton('Start connecting', 'button-primary', () => {
      state.activeIndex = 0;
      state.providerMode = 'guided';
      routeTo(state.plan.length === 0 ? 'goals' : 'provider');
    })
  );
  content.append(header, list, actions);
  return renderWizardFrame(1, content);
}

function renderProviderStep(profile) {
  if (!profile) {
    state.activeIndex = state.plan.length;
    return renderCompletion();
  }
  const copy = providerCopy(profile);
  const session = state.sessions.get(profile.id);
  const wrapper = element('section');
  const header = element('header', 'page-header');
  header.append(
    element(
      'p',
      'eyebrow',
      `Source ${Math.min(state.activeIndex + 1, state.plan.length)} of ${state.plan.length}`
    ),
    pageHeading(copy.name),
    element('p', 'page-copy', copy.purpose)
  );
  const card = element('article', 'provider-step');
  const hero = element('div', 'provider-hero');
  const mark = element('span', 'provider-mark', copy.mark);
  mark.setAttribute('aria-hidden', 'true');
  const heroCopy = element('div');
  heroCopy.append(element('h2', '', `Connect ${copy.name}`));
  const meta = element('div', 'provider-meta');
  meta.append(
    badge(copy.access, accessBadgeClass(copy.access)),
    badge(copy.effort, ''),
    statusBadge(profile)
  );
  heroCopy.append(meta);
  hero.append(mark, heroCopy);

  const body = element('div', 'provider-body');
  const examples = element('section');
  examples.append(element('h3', '', 'What you will get'));
  const exampleList = element('ul', 'example-list');
  copy.examples.forEach((example) => exampleList.append(element('li', '', example)));
  examples.append(exampleList);
  body.append(examples);

  if (isLocalProfile(profile)) {
    body.append(renderLocalGuidance(profile, copy));
  } else if (isConnected(session) && state.renewingProfile === profile.id) {
    body.append(renderRenewalAction(profile, session));
  } else if (isConnected(session)) {
    body.append(renderConnectedPanel(profile, session));
  } else if (!releaseAllowsSetup(profile)) {
    body.append(renderUnavailablePanel(profile));
  } else {
    body.append(renderProviderAction(profile, session));
  }

  body.append(
    explanatoryDetails('Why this source?', copy.account),
    technicalDetails(profile, session)
  );
  const navigation = element('div', 'selection-actions');
  navigation.append(
    actionButton(
      state.providerMode === 'advanced' ? 'Back to all providers' : 'Back',
      'button-quiet',
      previousProvider
    ),
    actionButton(
      state.providerMode === 'advanced'
        ? 'Done'
        : state.activeIndex + 1 >= state.plan.length
          ? 'Review setup'
          : 'Next source',
      '',
      nextProvider
    )
  );
  body.append(navigation);
  card.append(hero, body);
  wrapper.append(header, card);
  return renderWizardFrame(2, wrapper);
}

function renderLocalGuidance(profile, copy) {
  const panel = element('section', 'panel local-panel');
  panel.append(
    element('h3', '', 'Ready when you need it'),
    element(
      'p',
      'page-copy',
      `${copy.name} uses only local Market Squawk capabilities. There is no online signup or credential step.`
    )
  );
  const command = element('pre', 'technical-data', LOCAL_GUIDANCE[profile.id] || 'market-squawk --help');
  command.setAttribute('aria-label', 'Local command');
  panel.append(command);
  return panel;
}

function renderConnectedPanel(profile, session) {
  const panel = element('section', 'notice notice-success');
  panel.setAttribute('role', 'status');
  panel.append(
    element('span', 'notice-mark', '✓'),
    (() => {
      const copy = element('div');
      copy.append(
        element('h2', '', 'Connected and saved locally'),
        element(
          'p',
          '',
          `${providerCopy(profile).name} is active through the current provider authority.`
        )
      );
      return copy;
    })()
  );
  const providerDataset =
    profile.id === 'bls.v1-unregistered'
      ? state.providerDatasets.get(profile.id) ||
        (state.technical && state.technical.profile === profile.id
          ? state.technical.provider_dataset_identifier
          : null)
      : null;
  if (providerDataset) {
    const dataset = element('div', 'technical-block');
    dataset.append(
      element('h3', '', 'Your exact BLS dataset'),
      element(
        'p',
        '',
        'Keep this value for the release workflow. It is the exact dataset admitted by the BLS adapter.'
      ),
      element('pre', 'technical-data', providerDataset),
      element('pre', 'technical-data', `--bls-dataset ${providerDataset}`)
    );
    panel.append(dataset);
  }
  if (
    state.providerMode === 'advanced' &&
    session &&
    profile.credential_requirement === 'required_provider_controlled'
  ) {
    const renewal = renewalPresentation(profile);
    panel.append(
      actionButton(renewal.manageLabel, '', () => {
        state.renewingProfile = profile.id;
        render();
      })
    );
  }
  return panel;
}

function renderRenewalAction(profile, session) {
  const root = element('section', 'secret-panel');
  const renewal = renewalPresentation(profile);
  root.append(
    element('h3', '', renewal.title),
    element('p', 'page-copy', renewal.description),
    externalLink('Open the official provider page', renewal.handoffUrl)
  );
  const configuration = buildConfiguration(profile, true);
  if (!configuration) {
    root.append(renderUnavailablePanel(profile));
    return root;
  }
  root.append(configuration.root);
  const actions = element('div', 'button-row');
  actions.append(
    actionButton(renewal.continueLabel, 'button-primary', () =>
      startCredentialRenewal(profile, session, configuration)
    ),
    actionButton('Back without changing Market Squawk', 'button-quiet', () => {
      state.renewingProfile = null;
      render();
    })
  );
  root.append(actions);
  return root;
}

async function startCredentialRenewal(profile, session, configuration) {
  let adapterRequest;
  try {
    adapterRequest = await configuration.read();
  } catch (error) {
    if (error.message !== 'invalid_input') {
      presentError(error);
      render();
    }
    return;
  }
  state.pendingRequests.set(profile.id, adapterRequest);
  await runAction(async () => {
    const next = await mutate(
      `/api/v1/sessions/${session.session_id}/renew`,
      '{}',
      'application/json'
    );
    state.sessions.set(profile.id, next);
    await continueSession(profile, next, adapterRequest, 0);
  });
}

function renderUnavailablePanel(profile) {
  const release = releasePresentation(profile.release_state);
  const panel = element('section', 'notice notice-warning');
  panel.setAttribute('role', 'status');
  const copy = element('div');
  copy.append(
    element('h2', '', release.title),
    element(
      'p',
      '',
      `${release.explanation} This source remains visibly incomplete and blocks the capabilities that require it.`
    )
  );
  panel.append(element('span', 'notice-mark', '!'), copy);
  return panel;
}

function renderProviderAction(profile, session) {
  const root = element('section');
  if (session && secretAction(session.next_action)) {
    const resumedConfiguration = state.pendingRequests.has(profile.id)
      ? null
      : buildConfiguration(profile, state.providerMode === 'advanced');
    root.append(renderSecretStep(profile, session, resumedConfiguration));
    return root;
  }
  if (session && session.next_action === 'refresh_evidence') {
    root.append(renderUnavailablePanel(profile));
    return root;
  }
  if (session && session.next_action === 'resolve_rights') {
    root.append(renderUnavailablePanel(profile));
    return root;
  }

  const steps = element('section');
  steps.append(element('h3', '', 'What happens next'));
  const list = element('ol', 'steps-list');
  for (const step of setupSteps(profile)) {
    list.append(element('li', '', step));
  }
  steps.append(list);
  root.append(steps);

  const configuration = buildConfiguration(profile, state.providerMode === 'advanced');
  if (!configuration) {
    root.append(renderUnavailablePanel(profile));
    return root;
  }
  root.append(configuration.root);
  const contact = renderAdministrativeContact(profile);
  if (contact) root.append(contact.root);
  const actions = element('div', 'button-row');
  const label = session ? 'Continue setup' : primarySetupLabel(profile);
  actions.append(
    actionButton(label, 'button-primary', () => {
      beginProviderSetup(profile, configuration, contact, session);
    })
  );
  if (requiresProviderHandoff(profile) || providerCopy(profile).showOfficialLink) {
    actions.append(externalLink('Open official page', officialHandoffUrl(profile)));
  }
  root.append(actions);
  if (state.busy) root.append(renderBusyLine('Checking the provider and saving local state…'));
  return root;
}

function renderSecretStep(profile, session, resumedConfiguration) {
  const root = element('section', 'secret-panel');
  if (session.next_action === 'complete_provider_handoff') {
    const handoff = element('section', 'handoff-panel');
    handoff.append(
      element('h3', '', 'Complete the official provider step'),
      element('p', '', handoffInstruction(profile)),
      externalLink('Open the official provider page', officialHandoffUrl(profile))
    );
    root.append(handoff);
  }
  if (resumedConfiguration) {
    root.append(
      element('h3', '', 'Confirm the saved-session data settings'),
      element(
        'p',
        'field-hint',
        'This browser page was reopened, so confirm the non-secret provider settings before submitting the credential.'
      ),
      resumedConfiguration.root
    );
  }
  root.append(
    element('h3', '', `Add your ${providerCopy(profile).name} credential`),
    (() => {
      const note = element('div', 'secret-note');
      note.append(
        element('span', '', '✓'),
        element(
          'span',
          '',
          'The value is submitted directly to the local secret store. It is cleared from this page immediately and is never displayed in status output.'
        )
      );
      return note;
    })()
  );
  if (state.fallback === 'locked') root.append(renderFallbackPanel(false));

  const fields = [];
  if (profile.id === 'coinbase.exchange-direct-market-data') {
    fields.push(
      secretField('Coinbase API key', 'api-key', 1024),
      secretField('Coinbase passphrase', 'passphrase', 1024),
      secretField('Coinbase API secret key — shown once', 'signing-secret', 1024)
    );
  } else if (profile.id === 'alpaca.basic-market-data') {
    fields.push(
      secretField('Alpaca API key ID', 'alpaca-key-id', 4096),
      secretField('Alpaca secret key — shown once', 'alpaca-secret-key', 4096),
      selectField('Trading API realm', 'alpaca-trading-api-environment', [
        ['paper', 'Paper Trading — recommended for V1'],
        ['live', 'Live account credentials']
      ])
    );
  } else {
    const label =
      providerCopy(profile).credentialLabel || `${providerCopy(profile).name} API key`;
    fields.push(secretField(label, 'provider-key', 8192));
  }
  const form = element('div', 'form-grid');
  fields.forEach((field) => form.append(field.root));
  const submit = actionButton(
    credentialSubmitLabel(profile, session.next_action),
    'button-primary',
    () => submitProviderSecret(profile, session, fields, resumedConfiguration)
  );
  root.append(form, submit);
  if (state.busy) root.append(renderBusyLine('Verifying the credential without displaying it…'));
  return root;
}

function secretField(label, id, maximum) {
  const root = element('div', 'field field-full');
  const labelNode = element('label', '', label);
  labelNode.htmlFor = id;
  const input = document.createElement('input');
  input.id = id;
  input.type = 'password';
  input.required = true;
  input.maxLength = maximum;
  input.autocomplete = 'off';
  input.spellcheck = false;
  root.append(labelNode, input);
  return {root, input};
}

async function submitProviderSecret(profile, session, fields, resumedConfiguration) {
  if (resumedConfiguration) {
    try {
      state.pendingRequests.set(profile.id, await resumedConfiguration.read());
    } catch (error) {
      if (error.message !== 'invalid_input') {
        presentError(error);
        render();
      }
      return;
    }
  }
  if (!fields.every((field) => field.input.reportValidity())) return;
  let secret;
  if (profile.id === 'coinbase.exchange-direct-market-data') {
    secret = JSON.stringify({
      version: 1,
      api_key: fields[0].input.value,
      passphrase: fields[1].input.value,
      signing_secret: fields[2].input.value
    });
  } else if (profile.id === 'alpaca.basic-market-data') {
    secret = JSON.stringify({
      version: 1,
      key_id: fields[0].input.value,
      secret_key: fields[1].input.value,
      trading_api_environment: fields[2].input.value
    });
  } else {
    secret = fields[0].input.value;
  }
  fields.forEach((field) => {
    field.input.value = '';
    field.input.disabled = true;
  });
  await runAction(async () => {
    const next = await mutate(
      `/api/v1/sessions/${session.session_id}/secret`,
      secret,
      'application/octet-stream'
    );
    secret = '';
    state.sessions.set(profile.id, next);
    const request = state.pendingRequests.get(profile.id) || defaultActivationRequest(profile);
    await continueSession(profile, next, request, 0);
  });
  secret = '';
}

function buildConfiguration(profile, advanced) {
  if (
    profile.id === 'coinbase.public-market-data' ||
    profile.id === 'coinbase.exchange-direct-market-data' ||
    profile.id === 'alpaca.basic-market-data' ||
    profile.id === 'kraken.spot-public-market-data'
  ) {
    return staticConfiguration({kind: 'source'});
  }
  if (profile.id === 'sec.edgar-public') {
    return secConfiguration();
  }
  if (profile.id === 'fred-alfred.api-v1-v2') {
    return fredConfiguration(advanced);
  }
  if (profile.id === 'bls.v1-unregistered' || profile.id === 'bls.v2-registered') {
    return blsConfiguration(advanced);
  }
  if (profile.id === 'treasury.daily-rates-xml') {
    return treasuryDailyConfiguration();
  }
  if (profile.id === 'treasury.fiscal-data') {
    return treasuryFiscalConfiguration();
  }
  return null;
}

function staticConfiguration(request) {
  return {root: document.createDocumentFragment(), read: () => request};
}

function secConfiguration() {
  const root = element('fieldset');
  root.append(element('legend', 'field-label', 'Company to connect'));
  root.append(
    element(
      'p',
      'field-hint',
      'Enter the company’s exact 10-digit, zero-padded SEC CIK. Market Squawk uses it for both filings and reported financial facts.'
    )
  );
  const cik = textField('Company (SEC CIK)', 'sec-cik', '', 10);
  cik.input.required = true;
  cik.input.inputMode = 'numeric';
  cik.input.autocomplete = 'off';
  cik.input.pattern = '[0-9]{10}';
  cik.input.placeholder = '0000320193';
  root.append(cik.root);
  root.append(
    externalLink(
      'Find a company by name or ticker on SEC.gov',
      'https://www.sec.gov/search-filings'
    )
  );
  return {
    root,
    read: () => {
      if (!cik.input.reportValidity()) throw new Error('invalid_input');
      return {kind: 'sec', cik: cik.input.value};
    }
  };
}

function blsConfiguration(advanced) {
  const root = element('fieldset');
  root.append(element('legend', 'field-label', 'Starter data'));
  const intro = element(
    'p',
    'field-hint',
    'Starts with the monthly, seasonally adjusted U.S. unemployment rate.'
  );
  const grid = element('div', 'form-grid');
  const now = new Date().getUTCFullYear();
  const start = numberField('Start year', 'bls-start-year', now - 2, 1913, 9999);
  const end = numberField('End year', 'bls-end-year', now, 1913, 9999);
  grid.append(start.root, end.root);
  root.append(intro, grid);

  const custom = element('details');
  if (advanced) custom.open = true;
  const summary = element('summary', '', 'Advanced series details');
  const body = element('div', 'details-body');
  const rows = element('div', 'series-list');
  const seriesRows = [];
  function addSeries(seed) {
    const index = seriesRows.length;
    const row = element('fieldset', 'series-row');
    row.append(element('legend', 'field-label', `Series ${index + 1}`));
    const fields = {
      series: textField('Series ID', `bls-series-${index}`, seed.series, 50),
      title: textField('Verified title', `bls-title-${index}`, seed.title, 512),
      unit: textField('Unit', `bls-unit-${index}`, seed.unit, 128),
      frequency: textField('Frequency', `bls-frequency-${index}`, seed.frequency, 128),
      adjustment: textField(
        'Seasonal adjustment',
        `bls-adjustment-${index}`,
        seed.adjustment,
        128
      ),
      measure: textField('Measure', `bls-measure-${index}`, seed.measure, 128)
    };
    const grid = element('div', 'form-grid');
    grid.append(
      fields.series.root,
      fields.title.root,
      fields.unit.root,
      fields.frequency.root,
      fields.adjustment.root,
      fields.measure.root
    );
    row.append(grid);
    if (index > 0) {
      row.append(
        actionButton('Remove series', 'button-danger', () => {
          const position = seriesRows.findIndex((candidate) => candidate.row === row);
          if (position >= 0) seriesRows.splice(position, 1);
          row.remove();
        })
      );
    }
    seriesRows.push({...fields, row});
    rows.append(row);
  }
  addSeries({
    series: 'LNS14000000',
    title: 'Unemployment Rate',
    unit: 'percent',
    frequency: 'monthly',
    adjustment: 'seasonally-adjusted',
    measure: 'unemployment-rate'
  });
  const add = actionButton('Add another verified series', '', () => {
    addSeries({
      series: '',
      title: '',
      unit: '',
      frequency: '',
      adjustment: '',
      measure: ''
    });
  });
  body.append(rows, add);
  custom.append(summary, body);
  root.append(custom);
  return {
    root,
    read: () => {
      requireFields([start, end]);
      for (const row of seriesRows) {
        requireFields([
          row.series,
          row.title,
          row.unit,
          row.frequency,
          row.adjustment,
          row.measure
        ]);
      }
      const startYear = Number(start.input.value);
      const endYear = Number(end.input.value);
      if (startYear > endYear) {
        start.input.setCustomValidity('Start year must not be after end year.');
        start.input.reportValidity();
        start.input.setCustomValidity('');
        throw new Error('invalid_input');
      }
      return {
        kind: 'bls',
        start_year: startYear,
        end_year: endYear,
        series: seriesRows.map((row) => ({
          series_id: row.series.input.value,
          title: row.title.input.value,
          unit: row.unit.input.value,
          frequency: row.frequency.input.value,
          seasonal_adjustment: row.adjustment.input.value,
          measure: row.measure.input.value
        }))
      };
    }
  };
}

function fredConfiguration(advanced) {
  const root = element('fieldset');
  root.append(
    element('legend', 'field-label', 'Written permission and exact series authority')
  );

  const guidance = element('section', 'notice notice-info');
  const guidanceCopy = element('div');
  guidanceCopy.append(
    element('h3', '', 'Start with BLS unless you need FRED vintages'),
    element(
      'p',
      '',
      'The public BLS source provides unemployment data with no account or key. FRED is the advanced path for provider-reported vintages and requires a written St. Louis Fed permission response before Market Squawk can save or train on the data.'
    ),
    element(
      'p',
      'provider-legal-notice',
      'This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.'
    ),
    externalLink(
      'Read the FRED API terms',
      'https://fred.stlouisfed.org/docs/api/terms_of_use.html'
    ),
    externalLink('Open the official St. Louis Fed permission form', 'https://fred.stlouisfed.org/contactus/')
  );
  guidance.append(element('span', 'notice-mark', 'i'), guidanceCopy);
  root.append(guidance);

  const requestTemplate = [
    'Application: Market Squawk',
    'Service: FRED API',
    'Series: UNRATE',
    'Requested operations: local persistence, caching, archival, and model training',
    'Please confirm in writing whether the Federal Reserve Bank of St. Louis authorizes these exact operations for this application and series.'
  ].join('\n');
  const request = textareaField(
    'Permission request template',
    'fred-permission-template',
    'Copy this into the official permission form, then import the exact response you receive.'
  );
  request.input.value = requestTemplate;
  request.input.readOnly = true;
  const copyTemplate = actionButton('Copy permission request', 'button-quiet', async () => {
    try {
      await navigator.clipboard.writeText(requestTemplate);
      showNotice(
        'success',
        'Permission request copied',
        'Paste it into the official St. Louis Fed form.'
      );
    } catch (_error) {
      request.input.focus();
      request.input.select();
    }
  });
  root.append(request.root, copyTemplate);

  const permission = element('fieldset');
  permission.append(
    element('legend', 'field-label', '1. Import the exact St. Louis Fed response'),
    element(
      'p',
      'field-hint',
      'Choose the exact response downloaded from its official stlouisfed.org HTTPS URL. Market Squawk reacquires that URL and compares every byte before activation.'
    )
  );
  const permissionFile = fileField(
    'Exact permission response',
    'fred-service-permission-file',
    '.txt,.html,.pdf,text/plain,text/html,application/pdf,application/octet-stream'
  );
  const evidenceUrl = urlField('Exact response URL', 'fred-permission-evidence-url', '');
  const authorityUrl = urlField(
    'Official authority URL',
    'fred-permission-authority-url',
    'https://fred.stlouisfed.org/contactus/'
  );
  const channelGrid = element('div', 'form-grid');
  channelGrid.append(
    permissionFile.root,
    evidenceUrl.root,
    authorityUrl.root
  );
  permission.append(channelGrid);
  evidenceUrl.input.required = true;
  authorityUrl.input.required = true;
  root.append(permission);

  const review = element('fieldset');
  review.append(
    element('legend', 'field-label', '2. Record your local scope review'),
    element(
      'p',
      'field-hint',
      'Confirm only what the imported response actually authorizes. Market Squawk binds this decision to the response hash and current terms.'
    )
  );
  const reviewer = textField(
    'Reviewer identifier',
    'fred-permission-reviewer',
    'local-rights-reviewer',
    256
  );
  const effectiveDate = dateField(
    'Permission effective date',
    'fred-permission-effective',
    utcDateOffset(0)
  );
  const permissionExpiry = dateField(
    'Permission expiry date (if stated)',
    'fred-permission-expiry',
    ''
  );
  permissionExpiry.input.required = false;
  const revalidateDate = dateField(
    'Review again by',
    'fred-permission-revalidate',
    utcDateOffset(2)
  );
  const conditions = textareaField(
    'Conditions in the response (one per line)',
    'fred-permission-conditions',
    'Leave blank only when the response states no additional conditions.'
  );
  conditions.input.maxLength = 32768;
  const reviewGrid = element('div', 'form-grid');
  reviewGrid.append(
    reviewer.root,
    effectiveDate.root,
    permissionExpiry.root,
    revalidateDate.root
  );
  review.append(reviewGrid, conditions.root);
  const scopeConfirmation = checkboxField(
    'fred-permission-scope-confirmed',
    'I reviewed the exact response and it explicitly covers Market Squawk, the FRED API, this series, local persistence, caching, archival, and model training.'
  );
  review.append(scopeConfirmation.root);
  root.append(review);

  const seriesSection = element('fieldset');
  seriesSection.append(
    element('legend', 'field-label', '3. Confirm the exact series'),
    element(
      'p',
      'field-hint',
      'The guided starter uses FRED series UNRATE with the code-reviewed BLS public-domain decision.'
    )
  );
  const series = textField('FRED series ID', 'fred-series', 'UNRATE', 120);
  const owner = textField(
    'Series owner',
    'fred-owner',
    'us-bureau-of-labor-statistics',
    256
  );
  series.input.readOnly = true;
  owner.input.readOnly = true;
  const basis = selectField('Series authority', 'fred-rights-basis', [
    ['reviewed_unrate', 'Reviewed UNRATE public-domain decision']
  ]);
  const grantEffective = dateField(
    'Series authority effective date',
    'fred-grant-effective',
    utcDateOffset(0)
  );
  const grantExpiry = dateField(
    'Review series authority again by',
    'fred-grant-expiry',
    utcDateOffset(2)
  );
  const seriesGrid = element('div', 'form-grid');
  seriesGrid.append(
    series.root,
    owner.root,
    basis.root,
    grantEffective.root,
    grantExpiry.root
  );
  seriesSection.append(seriesGrid);
  root.append(seriesSection);

  return {
    root,
    read: async () => {
      requireFields([
        permissionFile,
        evidenceUrl,
        authorityUrl,
        reviewer,
        effectiveDate,
        revalidateDate,
        series,
        owner,
        basis,
        grantEffective,
        grantExpiry
      ]);
      if (!scopeConfirmation.input.reportValidity()) throw new Error('invalid_input');
      if (
        basis.input.value !== 'reviewed_unrate' ||
        series.input.value !== 'UNRATE' ||
        owner.input.value !== 'us-bureau-of-labor-statistics'
      ) {
        throwInvalidRange(
          series.input,
          'The reviewed starter decision is available only for UNRATE and its BLS owner.'
        );
      }

      const reviewedAt = currentUnixNanos();
      const permissionEffective = dateUnixNanos(effectiveDate.input.value);
      const revalidateBy = dateUnixNanos(revalidateDate.input.value);
      const permissionExpires = permissionExpiry.input.value
        ? dateUnixNanos(permissionExpiry.input.value)
        : null;
      const grantStarts = dateUnixNanos(grantEffective.input.value);
      const grantEnds = dateUnixNanos(grantExpiry.input.value);
      if (permissionEffective > reviewedAt || reviewedAt >= revalidateBy) {
        throwInvalidRange(
          revalidateDate.input,
          'The permission must already be effective and the review deadline must be in the future.'
        );
      }
      if (
        permissionExpires !== null &&
        (permissionExpires <= permissionEffective || permissionExpires <= reviewedAt)
      ) {
        throwInvalidRange(
          permissionExpiry.input,
          'The permission expiry must be after its effective date and still be in the future.'
        );
      }
      if (grantStarts >= grantEnds || grantEnds <= reviewedAt) {
        throwInvalidRange(
          grantExpiry.input,
          'The series-authority review deadline must be after its effective date and still be in the future.'
        );
      }

      const permissionBytes = await exactPortalEvidence(permissionFile.input.files[0]);
      const permissionChannel = {
        kind: 'official_https',
        evidence_url: evidenceUrl.input.value,
        authority_url: authorityUrl.input.value
      };
      const grantEvidence = {kind: 'reviewed_unrate'};
      const reviewedConditions = conditions.input.value
        .split('\n')
        .map((condition) => condition.trim())
        .filter(Boolean);
      if (
        reviewedConditions.length > 32 ||
        new Set(reviewedConditions).size !== reviewedConditions.length ||
        reviewedConditions.some((condition) => condition.length > 1024)
      ) {
        throwInvalidRange(
          conditions.input,
          'Use at most 32 distinct conditions, each no longer than 1,024 characters.'
        );
      }
      return {
        kind: 'fred_alfred',
        service_permission: {
          evidence: {
            channel: permissionChannel,
            ...permissionBytes
          },
          review: {
            reviewer: reviewer.input.value,
            reviewed_at_unix_nanos: reviewedAt.toString(),
            issuer: 'federal-reserve-bank-of-st-louis',
            application: 'market-squawk',
            service: 'fred-api',
            series: [series.input.value],
            operations: ['persist', 'cache', 'archive', 'train'],
            conditions: reviewedConditions,
            effective_at_unix_nanos: permissionEffective.toString(),
            expires_at_unix_nanos:
              permissionExpires === null ? null : permissionExpires.toString(),
            revalidate_by_unix_nanos: revalidateBy.toString()
          }
        },
        grants: [
          {
            series: series.input.value,
            owner: owner.input.value,
            evidence: grantEvidence,
            effective_at_unix_nanos: grantStarts.toString(),
            expires_at_unix_nanos: grantEnds.toString()
          }
        ]
      };
    }
  };
}

function treasuryDailyConfiguration() {
  const root = element('fieldset');
  root.append(
    element('legend', 'field-label', 'Date range'),
    element(
      'p',
      'field-hint',
      'The starter range covers the most recent five years through the current UTC year.'
    )
  );
  const current = new Date().getUTCFullYear();
  const grid = element('div', 'form-grid');
  const start = numberField('First year', 'treasury-start-year', current - 5, 1990, current);
  const end = numberField('Last year', 'treasury-end-year', current, 2003, current);
  grid.append(start.root, end.root);
  root.append(grid);
  return {
    root,
    read: () => {
      requireFields([start, end]);
      const startYear = Number(start.input.value);
      const endYear = Number(end.input.value);
      if (startYear > endYear) throwInvalidRange(start.input, 'First year must not follow last year.');
      return {kind: 'treasury_daily_rates', start_year: startYear, end_year: endYear};
    }
  };
}

function treasuryFiscalConfiguration() {
  const root = element('fieldset');
  root.append(
    element('legend', 'field-label', 'Date range'),
    element('p', 'field-hint', 'The starter range covers the previous twelve months.')
  );
  const endDate = new Date();
  const startDate = new Date(
    Date.UTC(endDate.getUTCFullYear() - 1, endDate.getUTCMonth(), endDate.getUTCDate())
  );
  const grid = element('div', 'form-grid');
  const start = dateField('First record date', 'fiscal-start', isoDate(startDate));
  const end = dateField('Last record date', 'fiscal-end', isoDate(endDate));
  grid.append(start.root, end.root);
  const details = element('details');
  const summary = element('summary', '', 'Advanced request options');
  const detailsBody = element('div', 'details-body form-grid');
  const pageSize = numberField('Provider page size', 'fiscal-page-size', 1000, 1, 10000);
  detailsBody.append(pageSize.root);
  details.append(summary, detailsBody);
  root.append(grid, details);
  return {
    root,
    read: () => {
      requireFields([start, end, pageSize]);
      if (start.input.value > end.input.value) {
        throwInvalidRange(start.input, 'First date must not follow last date.');
      }
      return {
        kind: 'treasury_fiscal',
        first_record_date: calendarDate(start.input.value),
        last_record_date: calendarDate(end.input.value),
        page_size: Number(pageSize.input.value)
      };
    }
  };
}

function renderAdministrativeContact(profile) {
  if (profile.administrative_contact_requirement !== 'required_non_secret') return null;
  const contactCopy = providerCopy(profile).contact || {
    legend: 'Provider contact',
    hint: 'Use a truthful organization and monitored email. These are not secret.',
    organizationLabel: 'Organization',
    emailLabel: 'Administrative email'
  };
  const root = element('fieldset');
  root.append(
    element('legend', 'field-label', contactCopy.legend),
    element('p', 'field-hint', contactCopy.hint)
  );
  const grid = element('div', 'form-grid');
  const organization = textField(
    contactCopy.organizationLabel,
    `${profile.id}-organization`,
    '',
    128
  );
  organization.input.autocomplete = 'organization';
  const email = emailField(contactCopy.emailLabel, `${profile.id}-email`, 128);
  grid.append(organization.root, email.root);
  root.append(grid);
  return {
    root,
    read: () => {
      requireFields([organization, email]);
      return {
        organization: organization.input.value,
        administrative_email: email.input.value
      };
    }
  };
}

async function beginProviderSetup(profile, configuration, contact, session) {
  let adapterRequest;
  let contactRequest = {};
  try {
    adapterRequest = await configuration.read();
    if (contact) contactRequest = contact.read();
  } catch (error) {
    if (error.message !== 'invalid_input') {
      presentError(error);
      render();
    }
    return;
  }
  state.pendingRequests.set(profile.id, adapterRequest);
  if (session) {
    await runAction(() => continueSession(profile, session, adapterRequest, 0));
    return;
  }
  if (requiresProviderHandoff(profile)) {
    window.open(officialHandoffUrl(profile), '_blank', 'noopener,noreferrer');
  }
  await runAction(async () => {
    const started = await mutate(
      '/api/v1/sessions',
      JSON.stringify({surface_id: profile.id, ...contactRequest}),
      'application/json'
    );
    state.sessions.set(profile.id, started);
    await continueSession(profile, started, adapterRequest, 0);
  });
}

async function continueSession(profile, session, adapterRequest, depth) {
  if (depth > 8) throw new PortalError('invalid_session_state', 409, session);
  state.sessions.set(profile.id, session);
  const action = session.next_action;
  if (action === 'active' || action === 'verify_and_activate' || action === 'verify_and_cutover') {
    const activated = await mutate(
      `/api/v1/sessions/${session.session_id}/activate`,
      JSON.stringify(adapterRequest),
      'application/json'
    );
    state.technical = activated;
    await refreshBootstrap();
    state.notice = {
      kind: 'success',
      title: `${providerCopy(profile).name} is connected`,
      message: 'The exact provider authority and configuration were saved locally.'
    };
    state.renewingProfile = null;
    return;
  }
  if (action === 'renew_credential') {
    const next = await mutate(
      `/api/v1/sessions/${session.session_id}/renew`,
      '{}',
      'application/json'
    );
    return continueSession(profile, next, adapterRequest, depth + 1);
  }
  if (action === 'reconcile_cleanup') {
    const next = await mutate(
      `/api/v1/sessions/${session.session_id}/cleanup`,
      '{}',
      'application/json'
    );
    return continueSession(profile, next, adapterRequest, depth + 1);
  }
  if (secretAction(action) || action === 'refresh_evidence' || action === 'resolve_rights') {
    return;
  }
  if (action === 'start_new_session') {
    state.notice = {
      kind: 'warning',
      title: 'Start a fresh provider session',
      message: 'The prior session is preserved for audit. Begin setup again to use current authority.'
    };
    return;
  }
  throw new PortalError('invalid_session_state', 409, session);
}

async function refreshBootstrap() {
  const response = await fetch('/api/v1/bootstrap', {
    method: 'GET',
    headers: {'accept': 'application/json'}
  });
  const body = await parseResponse(response);
  if (!response.ok) throw new PortalError(body.error || 'operation_unavailable', response.status, body);
  applyBootstrap(body);
}

async function mutate(path, body, contentType) {
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      'accept': 'application/json',
      'content-type': contentType,
      'x-csrf-token': state.csrf
    },
    body
  });
  const result = await parseResponse(response);
  state.technical = result;
  if (!response.ok) {
    throw new PortalError(result.error || 'operation_unavailable', response.status, result);
  }
  return result;
}

async function parseResponse(response) {
  try {
    return await response.json();
  } catch (_error) {
    return {error: 'operation_unavailable'};
  }
}

async function runAction(operation) {
  if (state.busy) return;
  state.busy = true;
  state.notice = null;
  render();
  try {
    await operation();
  } catch (error) {
    presentError(error);
  } finally {
    state.busy = false;
    render();
  }
}

function presentError(error) {
  const code = error instanceof PortalError ? error.code : 'operation_unavailable';
  const copy = ERROR_COPY[code] || ERROR_COPY.operation_unavailable;
  state.notice = {kind: 'error', title: copy[0], message: copy[1]};
  if (error instanceof PortalError) state.technical = error.detail;
  announce(`${copy[0]}. ${copy[1]}`);
}

function showNotice(kind, title, message) {
  state.notice = {kind, title, message};
  render();
  announce(`${title}. ${message}`);
}

function renderNotice() {
  const notice = element('section', `notice notice-${state.notice.kind}`);
  notice.setAttribute('role', state.notice.kind === 'error' ? 'alert' : 'status');
  const symbol = state.notice.kind === 'success' ? '✓' : state.notice.kind === 'warning' ? '!' : '×';
  const copy = element('div');
  copy.append(
    element('h2', '', state.notice.title),
    element('p', '', state.notice.message)
  );
  const close = element('button', 'notice-close', '×');
  close.type = 'button';
  close.setAttribute('aria-label', 'Dismiss message');
  close.addEventListener('click', () => {
    state.notice = null;
    render();
  });
  notice.append(element('span', 'notice-mark', symbol), copy, close);
  return notice;
}

function renderBusyLine(message) {
  const line = element('div', 'busy-line');
  const spinner = element('span', 'spinner');
  spinner.setAttribute('aria-hidden', 'true');
  line.append(spinner, element('span', '', message));
  return line;
}

function renderFallbackPanel(showReady) {
  const panel = element('section', 'fallback-panel');
  if (state.fallback === 'disabled') {
    panel.append(
      element('h3', '', 'Operating-system credential store'),
      element(
        'p',
        '',
        'Credentials will use the operating-system credential store. No encrypted file fallback is configured.'
      )
    );
    return panel;
  }
  if (state.fallback === 'locked') {
    panel.append(
      element('h3', '', 'Unlock the encrypted local credential store'),
      element(
        'p',
        '',
        'The unlock phrase goes only to this Market Squawk process and is cleared from the page immediately.'
      )
    );
    const secret = secretField('Local unlock phrase', 'fallback-unlock', 8192);
    secret.input.autocomplete = 'current-password';
    const button = actionButton('Unlock local credential store', '', async () => {
      if (!secret.input.reportValidity()) return;
      let value = secret.input.value;
      secret.input.value = '';
      secret.input.disabled = true;
      await runAction(async () => {
        const result = await mutate(
          '/api/v1/secrets/fallback/unlock',
          value,
          'application/octet-stream'
        );
        value = '';
        state.fallback = result.encrypted_file_fallback;
        state.notice = {
          kind: 'success',
          title: 'Local credential store unlocked',
          message: 'You can now submit provider credentials through the write-only field.'
        };
      });
      value = '';
    });
    panel.append(secret.root, button);
    return panel;
  }
  panel.append(
    element('h3', '', 'Encrypted local credential store is ready'),
    element('p', '', 'Provider credentials can be stored for this Market Squawk process.')
  );
  if (showReady) {
    panel.append(
      actionButton('Lock local credential store', '', () =>
        runAction(async () => {
          const result = await mutate(
            '/api/v1/secrets/fallback/lock',
            '{}',
            'application/json'
          );
          state.fallback = result.encrypted_file_fallback;
        })
      )
    );
  }
  return panel;
}

function technicalDetails(profile, session) {
  const details = element('details');
  const summary = element('summary', '', 'Technical details');
  const body = element('div', 'details-body');
  body.append(
    element(
      'p',
      '',
      'These identifiers and evidence fields are useful for audit and troubleshooting. No credential is included.'
    )
  );
  const machine = {
    provider_id: profile.id,
    release_state: profile.release_state,
    coverage: profile.coverage,
    quality_ceiling: profile.quality_ceiling,
    capability_revision: profile.capability_revision,
    capability_digest: profile.capability_digest,
    rights_state: profile.rights_state,
    rights_duties: profile.rights_duties,
    current_session: session || null,
    latest_response: state.technical
  };
  body.append(element('pre', 'technical-data', JSON.stringify(machine, null, 2)));
  details.append(summary, body);
  return details;
}

function explanatoryDetails(label, copy) {
  const details = element('details');
  const summary = element('summary', '', label);
  const body = element('div', 'details-body');
  body.append(element('p', '', copy));
  details.append(summary, body);
  return details;
}

function renderCompletion() {
  const content = element('section');
  const connected = state.plan.filter((profile) => isConnected(state.sessions.get(profile.id))).length;
  const local = state.plan.filter(isLocalProfile).length;
  const attention = state.plan.length - connected - local;
  const card = element('div', 'completion-card');
  const check = element('div', 'check-mark', attention === 0 ? '✓' : '→');
  check.setAttribute('aria-hidden', 'true');
  const heading = pageHeading(
    attention === 0 ? 'Your data setup is ready' : 'Your setup progress is saved'
  );
  card.append(
    check,
    element('p', 'eyebrow', 'Step 4 of 4'),
    heading,
    element(
      'p',
      'page-copy',
      attention === 0
        ? 'Market Squawk can now use the sources you connected while keeping their authority and provenance explicit.'
        : 'Connected sources are ready. Sources needing attention stay incomplete and can be finished from Advanced setup.'
    )
  );
  const summary = element('div', 'summary-grid');
  summary.append(
    summaryCard(connected, 'Connected online sources'),
    summaryCard(local, 'Local capabilities ready'),
    summaryCard(attention, 'Sources needing attention')
  );
  const actions = element('div', 'button-row');
  actions.append(
    actionButton('Finish setup', 'button-primary', () => {
      showNotice(
        'success',
        'Setup is saved locally',
        'You can close this tab or continue to Advanced setup at any time.'
      );
    }),
    actionButton('Review advanced settings', '', () => routeTo('advanced')),
    actionButton('Review sources again', 'button-quiet', () => {
      state.activeIndex = 0;
      routeTo(state.plan.length ? 'provider' : 'goals');
    })
  );
  card.append(summary, actions);
  content.append(card);
  return renderWizardFrame(3, content);
}

function summaryCard(number, label) {
  const card = element('div', 'summary-card');
  card.append(element('span', 'summary-number', String(number)), element('span', 'summary-label', label));
  return card;
}

function renderAdvanced() {
  const root = element('section');
  const header = element('header', 'page-header');
  header.append(
    element('p', 'eyebrow', 'Complete provider control'),
    pageHeading('Advanced setup'),
    element(
      'p',
      'page-copy',
      'Inspect every supported provider, manage durable sessions, and open specialist configuration only when you need it.'
    )
  );
  root.append(header, renderFallbackPanel(true));

  const toolbar = element('div', 'advanced-toolbar');
  const search = document.createElement('input');
  search.type = 'search';
  search.placeholder = 'Search providers';
  search.setAttribute('aria-label', 'Search providers');
  search.value = state.advancedFilter;
  search.addEventListener('input', () => {
    state.advancedFilter = search.value;
    renderAdvancedList(list);
  });
  toolbar.append(search, actionButton('Return to guided setup', '', () => routeTo(guidedReturnRoute())));
  const list = element('div', 'advanced-list');
  root.append(toolbar, list);
  renderAdvancedList(list);
  return root;
}

function renderAdvancedList(list) {
  list.replaceChildren();
  const query = state.advancedFilter.trim().toLocaleLowerCase();
  const profiles = state.profiles.filter((profile) => {
    const copy = providerCopy(profile);
    return (
      query.length === 0 ||
      copy.name.toLocaleLowerCase().includes(query) ||
      copy.purpose.toLocaleLowerCase().includes(query) ||
      profile.id.toLocaleLowerCase().includes(query)
    );
  });
  if (profiles.length === 0) {
    list.append(element('div', 'empty-state', 'No providers match that search.'));
    return;
  }
  for (const profile of profiles) list.append(renderAdvancedCard(profile));
}

function renderAdvancedCard(profile) {
  const copy = providerCopy(profile);
  const session = state.sessions.get(profile.id);
  const card = element('article', 'advanced-card');
  const header = element('div', 'advanced-card-header');
  const mark = element('span', 'provider-mark', copy.mark);
  mark.setAttribute('aria-hidden', 'true');
  const body = element('div', 'provider-copy');
  body.append(element('h2', '', copy.name), element('p', '', copy.purpose));
  const meta = element('div', 'provider-meta');
  meta.append(badge(copy.access, accessBadgeClass(copy.access)), statusBadge(profile));
  body.append(meta);
  const configure = actionButton(
    isLocalProfile(profile) ? 'View local steps' : session ? 'Continue or manage' : 'Configure',
    '',
    () => {
      state.plan = [profile];
      state.activeIndex = 0;
      state.providerMode = 'advanced';
      routeTo('provider');
    }
  );
  header.append(mark, body, configure);
  const details = element('details');
  const summary = element('summary', '', 'Provider controls and evidence');
  const detailsBody = element('div', 'advanced-card-body');
  detailsBody.append(
    element('p', '', copy.account),
    externalLink('Open official provider page', officialHandoffUrl(profile)),
    technicalDetails(profile, session)
  );
  if (session && isConnected(session) && profile.credential_requirement === 'required_provider_controlled') {
    const renewal = renewalPresentation(profile);
    detailsBody.append(
      actionButton(renewal.manageLabel, '', () => prepareRenewal(profile))
    );
  }
  if (session) {
    detailsBody.append(
      actionButton('Remove local provider authority', 'button-danger', () =>
        removeProviderAuthority(profile, session)
      )
    );
  }
  details.append(summary, detailsBody);
  card.append(header, details);
  return card;
}

function prepareRenewal(profile) {
  state.renewingProfile = profile.id;
  state.plan = [profile];
  state.activeIndex = 0;
  state.providerMode = 'advanced';
  routeTo('provider');
}

async function removeProviderAuthority(profile, session) {
  await runAction(async () => {
    const next = await mutate(
      `/api/v1/sessions/${session.session_id}/cancel`,
      '{}',
      'application/json'
    );
    state.sessions.set(profile.id, next);
    await refreshBootstrap();
    state.notice = {
      kind: 'success',
      title: `${providerCopy(profile).name} was removed locally`,
      message: 'The local runtime authority was revoked and the cleanup result was retained.'
    };
  });
}

function previousProvider() {
  if (state.providerMode === 'advanced') {
    state.renewingProfile = null;
    routeTo('advanced');
    return;
  }
  if (state.activeIndex === 0) {
    routeTo('review');
    return;
  }
  state.activeIndex -= 1;
  routeTo('provider');
}

function nextProvider() {
  if (state.providerMode === 'advanced') {
    state.renewingProfile = null;
    routeTo('advanced');
    return;
  }
  if (state.activeIndex + 1 >= state.plan.length) {
    state.activeIndex = state.plan.length;
    routeTo('completion');
    return;
  }
  state.activeIndex += 1;
  routeTo('provider');
}

function pageHeading(text) {
  const heading = element('h1', 'page-title', text);
  heading.dataset.pageHeading = '';
  return heading;
}

function badge(text, className) {
  return element('span', `badge ${className || ''}`.trim(), text);
}

function statusBadge(profile) {
  const session = state.sessions.get(profile.id);
  if (isConnected(session)) return badge('Connected', 'status-connected');
  if (profile.id === 'fred-alfred.api-v1-v2') {
    return badge('Written permission needed', 'status-attention');
  }
  const release = releasePresentation(profile.release_state);
  return badge(release.label, release.className);
}

function releasePresentation(release) {
  if (release === 'available' || release === 'rights_limited') {
    return {
      label: 'Ready to set up',
      title: 'Ready to set up',
      explanation: 'The current provider profile is admitted for its stated local use.',
      className: ''
    };
  }
  if (release === 'refresh_required') {
    return {
      label: 'Evidence refresh needed',
      title: 'Provider evidence must be refreshed',
      explanation: 'Current official provider evidence must be refreshed before activation.',
      className: 'status-attention'
    };
  }
  if (release === 'rights_blocked') {
    return {
      label: 'Rights decision needed',
      title: 'A rights decision is required',
      explanation: 'The source cannot be activated until its exact use is admitted.',
      className: 'status-unavailable'
    };
  }
  return {
    label: 'Unavailable',
    title: 'This provider is unavailable',
    explanation: 'The current release does not yet admit this exact provider capability.',
    className: 'status-unavailable'
  };
}

function providerCopy(profile) {
  return (
    PROVIDER_COPY[profile.id] || {
      mark: 'DATA',
      name: profile.display_name,
      purpose: profile.coverage || 'A code-supported Market Squawk data provider.',
      examples: [profile.coverage || 'Provider-declared coverage'],
      goals: [],
      effort: 'Advanced',
      access: accountLabel(profile),
      account: profile.handoff_instruction
    }
  );
}

function officialHandoffUrl(profile) {
  return providerCopy(profile).handoffUrl || profile.official_handoff_url;
}

function handoffInstruction(profile) {
  return providerCopy(profile).handoffInstruction || profile.handoff_instruction;
}

function setupSteps(profile) {
  const configured = providerCopy(profile).setupSteps;
  if (configured) return configured;
  if (requiresProviderHandoff(profile)) {
    return [
      'Open the exact official provider page.',
      'Complete the provider-controlled account or key step.',
      'Return here and submit the requested value once.',
      'Market Squawk verifies and activates the source locally.'
    ];
  }
  return [
    'Review the safe starter settings below.',
    'Market Squawk checks the official source.',
    'The exact provider configuration is saved locally.'
  ];
}

function renewalPresentation(profile) {
  const copy = providerCopy(profile);
  const configured = copy.renewal || {};
  return {
    manageLabel: configured.manageLabel || 'Replace credential',
    title: configured.title || `Replace the ${copy.name} credential`,
    description:
      configured.description ||
      'Complete the provider-controlled replacement step, then return with the credential that the provider tells you to use.',
    handoffUrl: configured.handoffUrl || officialHandoffUrl(profile),
    continueLabel: configured.continueLabel || 'Continue to replacement credential',
    submitLabel: configured.submitLabel || 'Import replacement and activate'
  };
}

function credentialSubmitLabel(profile, action) {
  const copy = providerCopy(profile);
  if (action === 'import_replacement') return renewalPresentation(profile).submitLabel;
  return copy.submitLabel || 'Save key and activate';
}

function accountLabel(profile) {
  if (profile.credential_requirement === 'required_provider_controlled') return 'API key required';
  if (profile.account_requirement === 'required_provider_controlled') return 'Free account';
  return 'No account';
}

function accessBadgeClass(access) {
  if (access === 'No account' || access === 'Local only') return 'badge-success';
  if (access.includes('key') || access === 'Contact details') return 'badge-warning';
  return '';
}

function isConnected(session) {
  return Boolean(
    session && session.next_action === 'active' && session.state === 'active_scoped'
  );
}

function isLocalProfile(profile) {
  return Object.prototype.hasOwnProperty.call(LOCAL_GUIDANCE, profile.id);
}

function releaseAllowsSetup(profile) {
  return profile.release_state === 'available' || profile.release_state === 'rights_limited';
}

function requiresProviderHandoff(profile) {
  return (
    profile.account_requirement === 'required_provider_controlled' ||
    profile.credential_requirement === 'required_provider_controlled'
  );
}

function secretAction(action) {
  return (
    action === 'complete_provider_handoff' ||
    action === 'import_secret' ||
    action === 'import_replacement'
  );
}

function primarySetupLabel(profile) {
  if (profile.id === 'fred-alfred.api-v1-v2') return 'Review permission and connect';
  if (requiresProviderHandoff(profile)) return 'Start guided connection';
  return 'Connect this source';
}

function defaultActivationRequest(profile) {
  if (
    profile.id === 'coinbase.public-market-data' ||
    profile.id === 'coinbase.exchange-direct-market-data' ||
    profile.id === 'alpaca.basic-market-data' ||
    profile.id === 'kraken.spot-public-market-data'
  ) {
    return {kind: 'source'};
  }
  return null;
}

function textField(label, id, value, maximum) {
  return inputField(label, id, 'text', value, maximum);
}

function emailField(label, id, maximum) {
  return inputField(label, id, 'email', '', maximum);
}

function urlField(label, id, value) {
  const field = inputField(label, id, 'url', value, 2048);
  field.input.pattern = 'https://.*';
  return field;
}

function dateField(label, id, value) {
  return inputField(label, id, 'date', value);
}

function fileField(label, id, accept) {
  const field = inputField(label, id, 'file', '');
  field.input.accept = accept;
  return field;
}

function checkboxField(id, label) {
  const root = element('label', 'checkbox-field field-full');
  const input = document.createElement('input');
  input.id = id;
  input.type = 'checkbox';
  input.required = true;
  root.append(input, element('span', '', label));
  return {root, input};
}

function numberField(label, id, value, minimum, maximum) {
  const field = inputField(label, id, 'number', String(value));
  field.input.min = String(minimum);
  field.input.max = String(maximum);
  field.input.step = '1';
  return field;
}

function inputField(label, id, type, value, maximum) {
  const root = element('div', 'field');
  const labelNode = element('label', '', label);
  labelNode.htmlFor = id;
  const input = document.createElement('input');
  input.id = id;
  input.type = type;
  input.required = true;
  input.value = value;
  if (maximum) input.maxLength = maximum;
  root.append(labelNode, input);
  return {root, input};
}

function selectField(label, id, options) {
  const root = element('div', 'field');
  const labelNode = element('label', '', label);
  labelNode.htmlFor = id;
  const input = document.createElement('select');
  input.id = id;
  input.required = true;
  options.forEach(([value, copy]) => {
    const option = element('option', '', copy);
    option.value = value;
    input.append(option);
  });
  root.append(labelNode, input);
  return {root, input};
}

function textareaField(label, id, hint) {
  const root = element('div', 'field field-full');
  const labelNode = element('label', '', label);
  labelNode.htmlFor = id;
  const input = document.createElement('textarea');
  input.id = id;
  input.maxLength = 262144;
  input.setAttribute('aria-describedby', `${id}-hint`);
  const hintNode = element('p', 'field-hint', hint);
  hintNode.id = `${id}-hint`;
  root.append(labelNode, input, hintNode);
  return {root, input};
}

function requireFields(fields) {
  for (const field of fields) {
    if (!field.input.reportValidity()) throw new Error('invalid_input');
  }
}

function throwInvalidRange(input, message) {
  input.setCustomValidity(message);
  input.reportValidity();
  input.setCustomValidity('');
  throw new Error('invalid_input');
}

function calendarDate(value) {
  const [year, month, day] = value.split('-').map(Number);
  return {year, month, day};
}

function isoDate(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function utcDateOffset(days) {
  const date = new Date();
  date.setUTCHours(0, 0, 0, 0);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function currentUnixNanos() {
  return BigInt(Date.now()) * 1000000n;
}

function dateUnixNanos(value) {
  const milliseconds = Date.parse(`${value}T00:00:00.000Z`);
  if (!Number.isSafeInteger(milliseconds)) throw new Error('invalid_input');
  return BigInt(milliseconds) * 1000000n;
}

async function exactPortalEvidence(file) {
  const maximumBytes = 256 * 1024;
  if (!(file instanceof File) || file.size === 0 || file.size > maximumBytes) {
    throw new PortalError('invalid_adapter_request', 400, {
      message: 'Evidence files must contain 1 to 262,144 bytes.'
    });
  }
  let bytes;
  let digest;
  try {
    bytes = new Uint8Array(await file.arrayBuffer());
    digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  } catch (_error) {
    throw new PortalError('invalid_adapter_request', 400, {
      message: 'The selected evidence file could not be read and hashed locally.'
    });
  }
  return {
    sha256: Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join(''),
    byte_length: bytes.byteLength,
    content_base64: bytesToBase64(bytes)
  };
}

function bytesToBase64(bytes) {
  const chunkSize = 32768;
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
  }
  return btoa(binary);
}

function applyBootstrap(data) {
  state.csrf = data.csrf_token;
  state.profiles = Array.isArray(data.profiles) ? data.profiles : [];
  state.sessions = new Map(
    (Array.isArray(data.sessions) ? data.sessions : []).map((session) => [
      session.surface_id,
      session
    ])
  );
  state.providerDatasets = new Map(
    (Array.isArray(data.provider_datasets) ? data.provider_datasets : [])
      .filter(
        (entry) =>
          entry &&
          typeof entry.profile === 'string' &&
          typeof entry.provider_dataset_identifier === 'string'
      )
      .map((entry) => [entry.profile, entry.provider_dataset_identifier])
  );
  state.fallback = data.encrypted_file_fallback;
  if (state.plan.length > 0) {
    const byId = new Map(state.profiles.map((profile) => [profile.id, profile]));
    state.plan = state.plan.map((profile) => byId.get(profile.id)).filter(Boolean);
  }
}

async function bootstrap() {
  try {
    const response = await fetch('/api/v1/bootstrap', {
      method: 'GET',
      headers: {'accept': 'application/json'}
    });
    const data = await parseResponse(response);
    if (!response.ok) throw new PortalError(data.error || 'operation_unavailable', response.status, data);
    applyBootstrap(data);
  } catch (error) {
    presentError(error);
  }
  render();
}

bootstrap();
