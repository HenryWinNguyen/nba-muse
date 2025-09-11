// src/scripts/lib/runQuery.ts
import Database from 'better-sqlite3';

const DB_PATH = 'data/serving/nba.sqlite';

export type Player = { player_id: number; player_name: string };

type StatRow = {
  games: number;
  ppg: number | null; rpg: number | null; apg: number | null;
  spg: number | null; bpg: number | null; tov: number | null;
  fg_pct: number | null; fg3_pct: number | null; ft_pct: number | null;
  fgm: number | null; fga: number | null; fg3m: number | null; fg3a: number | null; ftm: number | null; fta: number | null;
};

export type Parsed = {
  playerName: string;
  oppAbbrs: string[] | null;
  n: number | null;              // null => career
  seasonType: 'playoffs' | 'regular' | null;
  dateFrom: string | null;
  dateTo: string | null;
  statKey?: 'fg_pct' | 'fg3_pct' | 'ft_pct' | 'ppg' | 'rpg' | 'apg' | 'spg' | 'bpg' | 'tov';
};

/* ---------------- helpers ---------------- */

function normalizeNameTokens(name: string): string[] {
  const norm = name.toLowerCase().replace(/\s+/g, ' ').trim();
  const tokens = norm.split(' ').filter(Boolean);
  const alias: Record<string, string> = {
    steph: 'stephen',
    bron: 'lebron',
    mike: 'michael',
    kd: 'kevin',
    dame: 'damian',
    cp3: 'chris',
    book: 'devin',
    pg: 'paul',
    tatum: 'jayson',
  };
  return tokens.map(t => alias[t] ?? t);
}

const TEAM_ALIASES: Record<string, string[] | string> = {
    // Warriors / Wizards / Lakers / Celtics / Bulls / Knicks
    GSW: ['GSW'], WARRIORS: ['GSW'], 'GOLDEN STATE': ['GSW'],
    WAS: ['WAS','WSB'], WIZARDS: ['WAS','WSB'], 'WASHINGTON WIZARDS': ['WAS','WSB'], WASHINGTON: ['WAS','WSB'],
    WSB: ['WSB','WAS'], BULLETS: ['WSB','WAS'],
  
    LAL: ['LAL'], LAKERS: ['LAL'], 'LOS ANGELES LAKERS': ['LAL'],
    BOS: ['BOS'], CELTICS: ['BOS'],
    CHI: ['CHI'], BULLS: ['CHI'],
    NYK: ['NYK'], KNICKS: ['NYK'], 'NEW YORK': ['NYK'],
  
    // Nets (Brooklyn/New Jersey)
    BKN: ['BKN','NJN'], NETS: ['BKN','NJN'], 'BROOKLYN NETS': ['BKN','NJN'],
    NJN: ['NJN','BKN'], 'NEW JERSEY NETS': ['NJN','BKN'],
  
    // Hornets (Charlotte: CHH ↔ CHA; Bobcats folded into CHA)
    CHA: ['CHA','CHH'], HORNETS: ['CHA','CHH'], 'CHARLOTTE HORNETS': ['CHA','CHH'],
    CHH: ['CHH','CHA'], BOBCATS: ['CHA'],
  
    // Pelicans (New Orleans: NOP ↔ NOH/NOK)
    NOP: ['NOP','NOH','NOK'], PELICANS: ['NOP','NOH','NOK'], 'NEW ORLEANS': ['NOP','NOH','NOK'],
    NOH: ['NOH','NOP','NOK'], NOK: ['NOK','NOP','NOH'],
  
    // Grizzlies (Memphis/Vancouver)
    MEM: ['MEM','VAN'], GRIZZLIES: ['MEM','VAN'],
    VAN: ['VAN','MEM'], 'VANCOUVER GRIZZLIES': ['VAN','MEM'],
  
    // Thunder / Sonics
    OKC: ['OKC','SEA'], THUNDER: ['OKC','SEA'],
    SEA: ['SEA','OKC'], SONICS: ['SEA','OKC'],
  
    // Suns / Sixers / Blazers / Cavs / Heat / Spurs / Mavs / Nuggets / Bucks / Raptors
    PHX: ['PHX'], SUNS: ['PHX'],
    PHI: ['PHI'], SIXERS: ['PHI'], '76ERS': ['PHI'], PHI76ERS: ['PHI'],
    POR: ['POR'], 'TRAIL BLAZERS': ['POR'], BLAZERS: ['POR'],
    CLE: ['CLE'], CAVS: ['CLE'], CAVALIERS: ['CLE'],
    MIA: ['MIA'], HEAT: ['MIA'],
    SAS: ['SAS'], SPURS: ['SAS'],
    DAL: ['DAL'], MAVS: ['DAL'], MAVERICKS: ['DAL'],
    DEN: ['DEN'], NUGGETS: ['DEN'],
    MIL: ['MIL'], BUCKS: ['MIL'],
    TOR: ['TOR'], RAPTORS: ['TOR'],
    ORL: ['ORL'], MAGIC: ['ORL'],
    DET: ['DET'], PISTONS: ['DET'],
    IND: ['IND'], PACERS: ['IND'],
    ATL: ['ATL'], HAWKS: ['ATL'],
    UTA: ['UTA'], JAZZ: ['UTA'],
    MIN: ['MIN'], TIMBERWOLVES: ['MIN'], WOLVES: ['MIN'],
    SAC: ['SAC'], KINGS: ['SAC'],
  };
  

function normalizeOpponent(raw: string | null): string[] | null {
    if (!raw) return null;
  
    // normalize punctuation/spaces and drop a leading "THE "
    let key = raw.toUpperCase()
      .replace(/\./g, '')
      .replace(/\s+/g, ' ')
      .trim();
  
    key = key.replace(/^THE\s+/, ''); // <-- handle "the Knicks", "the Lakers", etc.
  
    // minor niceties: strip trailing "TEAM", e.g., "BOSTON CELTICS TEAM"
    key = key.replace(/\s+TEAM$/, '');
  
    // try as-is, then try singularizing a simple trailing S
    const hit = TEAM_ALIASES[key] ?? TEAM_ALIASES[key.replace(/S$/, '')];
  
    if (!hit) {
      // if user typed full city name (e.g., "THE NEW YORK"), try last token
      const lastWord = key.split(' ').pop()!;
      const fallback = TEAM_ALIASES[lastWord] ?? TEAM_ALIASES[lastWord.replace(/S$/, '')];
      if (!fallback) return /^[A-Z]{2,4}$/.test(key) ? [key] : null;
      return Array.isArray(fallback) ? fallback : [fallback];
    }
  
    return Array.isArray(hit) ? hit : [hit];
  }
  

const WORD_NUM: Record<string, number> = {
  one:1, two:2, three:3, four:4, five:5, six:6, seven:7, eight:8, nine:9, ten:10,
  eleven:11, twelve:12, thirteen:13, fourteen:14, fifteen:15, sixteen:16, seventeen:17, eighteen:18, nineteen:19,
  twenty:20, thirty:30, forty:40, fifty:50, sixty:60, seventy:70, eighty:80, ninety:90
};
function parseWordNumber(s: string): number | null {
  const words = s.toLowerCase().trim().split(/\s+/);
  let total = 0;
  for (const w of words) { if (!(w in WORD_NUM)) return null; total += WORD_NUM[w]; }
  return total || null;
}

/* ---------------- parsing & resolution ---------------- */

export function parseInput(input: string): Parsed {
  let s = input.trim();

  // ---------- detect stat focus ----------
  type StatKey = NonNullable<Parsed['statKey']>;
  const STAT_PATTERNS: Array<[StatKey, RegExp[]]> = [
    ['fg3_pct', [/\b(?:three[-\s]?point|3p|3pt)\s*(?:percentage|pct|%)?\b/i]],
    ['fg_pct',  [/\bfg\s*%?\b/i, /\bfield\s*goal\s*(?:percentage|pct|%)\b/i]],
    ['ft_pct',  [/\bft\s*%?\b/i, /\bfree\s*throw\s*(?:percentage|pct|%)\b/i]],

    ['ppg',     [/\bpoints?\b/i, /\bpts\b/i]],
    ['rpg',     [/\brebounds?\b/i, /\breb\b/i]],
    ['apg',     [/\bassists?\b/i, /\bassists?\b/i, /\bast\b/i]], // tolerate misspellings + AST
    ['spg',     [/\bsteals?\b/i, /\bstl\b/i]],
    ['bpg',     [/\bblocks?\b/i, /\bblk\b/i]],
    ['tov',     [/\bturnovers?\b/i, /\btov\b/i]],
  ];

  const findStatKey = (text: string): StatKey | undefined => {
    for (const [key, regs] of STAT_PATTERNS) {
      if (regs.some(r => r.test(text))) return key;
    }
    return undefined;
  };

  const statKey = findStatKey(s);

  // we use these generic “stat-ish” tails to help cut the player name cleanly
  const statPhraseRe =
    /\b(?:(?:three[-\s]?point|3p|3pt)\s*(?:percentage|pct|%?)|fg\s*%?|field\s*goal\s*(?:percentage|pct|%?)|free\s*throw\s*(?:percentage|pct|%?)|ft\s*%?|points?|rebounds?|assists?|steals?|blocks?|turnovers?|3pt%|3p%|ft%|fg%)\b/i;

  // ---------------- season type + career handling ----------------
  let seasonType: Parsed['seasonType'] = null;
  let n: number | null = 10;

  if (/\bplayoffs?\b/i.test(s)) seasonType = 'playoffs';
  if (/\bregular\b/i.test(s))  seasonType = 'regular';

  if (/\bcareer\b/i.test(s)) n = null;

  // digits: "last 8" or "last 8 games"
    // words:  "last twenty four" or "last twenty four games"
    if (n !== null) {
        const nDigits = s.match(/\blast\s+(\d+)(?:\s+games?)?\b/i);
        if (nDigits) {
         n = Math.max(1, Math.min(400, parseInt(nDigits[1], 10)));
        } else {
         const nWords = s.match(/\blast\s+([a-z\s-]+?)(?:\s+games?)?\b/i);
         if (nWords) {
          const maybe = parseWordNumber(nWords[1].replace(/-/g, ' '));
          if (maybe) n = Math.max(1, Math.min(400, maybe));
      }
    }
  }
  

    // ---------------- opponent parsing ----------------
let oppRaw: string | null = null;
// non-greedy capture that stops before a control word or end-of-string
const vsMatch = s.match(
  /\b(?:vs|against)\s+([A-Za-z\.\s]{2,30}?)(?=\s+(?:last|career|playoffs?|regular|since|between)\b|$)/i
);
if (vsMatch) oppRaw = vsMatch[1].trim().replace(/^the\s+/i, ''); // strip leading "the"
const oppAbbrs = normalizeOpponent(oppRaw);



  // ---------------- date filters ----------------
  let dateFrom: string | null = null;
  let dateTo: string | null = null;

  const since = s.match(/\bsince\s+(\d{4})\b/i);
  if (since) dateFrom = `${since[1]}-01-01`;

  const between = s.match(/\bbetween\s+(\d{4})\s+and\s+(\d{4})\b/i);
  if (between) {
    const y1 = Math.min(parseInt(between[1], 10), parseInt(between[2], 10));
    const y2 = Math.max(parseInt(between[1], 10), parseInt(between[2], 10));
    dateFrom = `${y1}-01-01`;
    dateTo = `${y2}-12-31`;
  }

  // ---------------- player name extraction ----------------
  const indices: number[] = [];
  const pushIf = (m: RegExpMatchArray | null) => { if (m && m.index !== undefined) indices.push(m.index); };

  pushIf(s.match(/\s+vs\s+/i));
  pushIf(s.match(/\s+against\s+/i));
  pushIf(s.match(/\s+last\s+/i));
  pushIf(s.match(/\s+career\b/i));
  pushIf(s.match(/\s+playoffs?\b/i));
  pushIf(s.match(/\s+regular\b/i));
  pushIf(s.match(/\s+since\s+/i));
  pushIf(s.match(/\s+between\s+/i));
  pushIf(s.match(statPhraseRe));

  const cutIdx = indices.length ? Math.min(...indices) : s.length;
  let playerName = s.slice(0, cutIdx).trim();

  // If query is simply "<name> <stat words>", strip the trailing stat words
  if (playerName.length === s.length) {
    playerName = playerName.replace(
      /\b(three[-\s]?point|3p|3pt|fg|field\s*goal|free\s*throw|ft|points?|rebounds?|assists?|steals?|blocks?|turnovers?)\s*(percentage|pct|%)?$/i,
      ''
    ).trim();
  }

  // If a stat phrase is present and there’s no explicit window, default to career
  // If there's an explicit date window and no "last N", include all games in that window
const hasExplicitWindow =
/\blast\s+\d+/i.test(s) ||
/\bsince\s+\d{4}\b/i.test(s) ||
/\bbetween\s+\d{4}\s+and\s+\d{4}\b/i.test(s);

if (!/\blast\s+\d+/i.test(s) && (/\bsince\s+\d{4}\b/i.test(s) || /\bbetween\s+\d{4}\s+and\s+\d{4}\b/i.test(s))) {
n = null; // all games in the window
}


  return { playerName, oppAbbrs, n, seasonType, dateFrom, dateTo, statKey };
}

export function resolvePlayer(db: Database.Database, playerName: string): Player {
  const exact = db.prepare(
    `SELECT player_id, player_name FROM players WHERE player_name = ? LIMIT 1`
  ).get(playerName) as Player | undefined;
  if (exact) return exact;

  const tokens = normalizeNameTokens(playerName);
  if (tokens.length === 0) throw new Error('Empty player name');

  const where = tokens.map((_t, i) => `lower(player_name) LIKE @t${i}`).join(' AND ');
  const params: Record<string, string> = {};
  tokens.forEach((t, i) => (params[`t${i}`] = `%${t}%`));

  const matches = db.prepare(
    `SELECT player_id, player_name
     FROM players
     WHERE ${where}
     ORDER BY player_name
     LIMIT 6`
  ).all(params) as Player[];

  if (matches.length === 0) throw new Error(`No player found matching "${playerName}".`);
  if (matches.length === 1) return matches[0];

  const suggestions = matches.map(m => `  - ${m.player_name}`).join('\n');
  throw new Error(`Multiple players matched "${playerName}". Try one of:\n${suggestions}`);
}

/* ---------------- build WHERE once and reuse ---------------- */

export function buildQuery(text: string) {
  const parsed = parseInput(text);
  const { playerName, oppAbbrs, n, seasonType, dateFrom, dateTo } = parsed;

  const db = new Database(DB_PATH, { readonly: true });
  try {
    const player = resolvePlayer(db, playerName);

    const where: string[] = [`player_id = @pid`];
    const params: Record<string, any> = { pid: player.player_id };

    if (oppAbbrs && oppAbbrs.length > 0) {
      const ph = oppAbbrs.map((_, i) => `@opp${i}`).join(', ');
      where.push(`opponent_abbr IN (${ph})`);
      oppAbbrs.forEach((v, i) => (params[`opp${i}`] = v));
    }

    // --- detect available columns once per call
    const cols = new Set(
      db.prepare(`PRAGMA table_info(box_scores)`).all()
        .map((r: any) => String(r.name).toLowerCase())
    );

    if (seasonType) {
      // Accept many schemas and wordings
      const candidateCols = ['season_type', 'type', 'season', 'stage'];
      const presentCols = candidateCols.filter(c => cols.has(c));

      if (presentCols.length > 0) {
        const isPlayoffs = seasonType === 'playoffs';
        const likeParam = isPlayoffs ? '%playoff%' : '%regular%';

        const clauses: string[] = [];
        presentCols.forEach((c, i) => {
          const key = `@season_like_${i}`;
          params[`season_like_${i}`] = likeParam;
          clauses.push(`lower(${c}) LIKE ${key}`);
        });

        where.push(clauses.length === 1 ? clauses[0] : `(${clauses.join(' OR ')})`);
      }
    }

    if (dateFrom)   { where.push(`game_date >= @from`); params.from = dateFrom; }
    if (dateTo)     { where.push(`game_date <= @to`);   params.to = dateTo; }

    const subQuery =
      n == null
        ? `SELECT * FROM box_scores WHERE ${where.join(' AND ')}`
        : `SELECT * FROM box_scores WHERE ${where.join(' AND ')} ORDER BY game_date DESC LIMIT @n`;

    if (n != null) params.n = n;

    const ctxOpp = oppAbbrs ? `vs ${oppAbbrs.join('/')}` : '';
    const ctxRange =
      dateFrom && dateTo ? ` between ${dateFrom.slice(0,4)}–${dateTo.slice(0,4)}` :
      dateFrom ? ` since ${dateFrom.slice(0,4)}` : '';
    const ctxType = seasonType ? ` ${seasonType}` : '';
    const ctxSpan = n == null ? ' (career)' : '';

    const context = `${ctxOpp} ${ctxSpan}${ctxType}${ctxRange}`.trim();

    return { db, subQuery, params, player, context, parsed };
  } catch (e) {
    db.close();
    throw e;
  }
}

/* ---------------- public API: text + per-game ---------------- */

export function runQuery(text: string): string {
  const { db, subQuery, params, player, context, parsed } = buildQuery(text);
  try {
    const agg = db.prepare(
      `SELECT
         COUNT(*) AS games,
         AVG(PTS) AS ppg,
         AVG(REB) AS rpg,
         AVG(AST) AS apg,
         AVG(STL) AS spg,
         AVG(BLK) AS bpg,
         AVG(TOV) AS tov,
         AVG(FG_PCT) AS fg_pct,
         AVG(FG3_PCT) AS fg3_pct,
         AVG(FT_PCT) AS ft_pct,
         AVG(FGM) AS fgm, AVG(FGA) AS fga,
         AVG(FG3M) AS fg3m, AVG(FG3A) AS fg3a,
         AVG(FTM) AS ftm, AVG(FTA) AS fta
       FROM (${subQuery})`
    ).get(params) as StatRow;

    if (!agg || agg.games === 0) {
      return `No games found for ${player.player_name}${context ? ' ' + context : ''}.`;
    }

    const fmt = (x: number | null, d = 1) => (x == null ? '-' : Number(x).toFixed(d));
    const pct = (x: number | null) => (x == null ? '-' : Number(x).toFixed(1) + '%');

    // stat-focus single line
    if (parsed.statKey) {
      const LABELS: Record<NonNullable<Parsed['statKey']>, string> = {
        fg_pct: 'FG%', fg3_pct: '3P%', ft_pct: 'FT%',
        ppg: 'PPG', rpg: 'RPG', apg: 'APG', spg: 'SPG', bpg: 'BPG', tov: 'TOV',
      };
      const k = parsed.statKey;
      const value =
        k === 'fg_pct'  ? pct(agg.fg_pct)  :
        k === 'fg3_pct' ? pct(agg.fg3_pct) :
        k === 'ft_pct'  ? pct(agg.ft_pct)  :
        k === 'ppg'     ? fmt(agg.ppg)     :
        k === 'rpg'     ? fmt(agg.rpg)     :
        k === 'apg'     ? fmt(agg.apg)     :
        k === 'spg'     ? fmt(agg.spg)     :
        k === 'bpg'     ? fmt(agg.bpg)     :
                          fmt(agg.tov);

      const pureCareer = parsed.n === null && !parsed.dateFrom && !parsed.dateTo;
      const gamesNote = pureCareer ? ` (${agg.games} games)` : (agg.games ? ` (last ${agg.games} games)` : '');

      return `${player.player_name} ${context ? context : ''}${gamesNote}: ${LABELS[k]} ${value}`;
    }

    // default multi-stat block
    const span = agg.games ? ` (last ${agg.games} games)` : '';
    return `${player.player_name} ${context ? context : ''}${span}:
PPG ${fmt(agg.ppg)} | APG ${fmt(agg.apg)} | RPG ${fmt(agg.rpg)} | SPG ${fmt(agg.spg)} | BPG ${fmt(agg.bpg)} | TOV ${fmt(agg.tov)}
FG% ${pct(agg.fg_pct)} | 3P% ${pct(agg.fg3_pct)} | FT% ${pct(agg.ft_pct)}
FGM/FGA ${fmt(agg.fgm)}/${fmt(agg.fga)}, 3PM/3PA ${fmt(agg.fg3m)}/${fmt(agg.fg3a)}, FTM/FTA ${fmt(agg.ftm)}/${fmt(agg.fta)}`;
  } finally {
    db.close();
  }
}

export type GameRow = {
  game_date: string; opponent_abbr: string; team_abbr: string;
  MIN: string | null;
  PTS: number | null; REB: number | null; AST: number | null; STL: number | null; BLK: number | null; TOV: number | null;
  FG_PCT: number | null; FG3_PCT: number | null; FT_PCT: number | null;
};

export function runGames(
    text: string,
    limit = 25
  ): { player: string; rows: GameRow[]; career: boolean } {
    const { db, subQuery, params, player, parsed } = buildQuery(text);
    try {
      // If user specified "last N", use that as the default table window.
      const effectiveLimit = parsed.n ?? limit;
  
      const rows = db.prepare(
        `SELECT game_date, opponent_abbr, team_abbr,
                MIN, PTS, REB, AST, STL, BLK, TOV, FG_PCT, FG3_PCT, FT_PCT
         FROM (${subQuery})
         ORDER BY game_date DESC
         LIMIT @lim`
      ).all({ ...params, lim: Math.max(1, Math.min(400, effectiveLimit)) }) as GameRow[];
  
      return {
        player: player.player_name,
        rows,
        career: parsed.n === null && !parsed.dateFrom && !parsed.dateTo
      };
      
    } finally {
      db.close();
    }
  }
  
