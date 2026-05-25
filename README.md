# polyrec — Polymarket BTC 15-min calibration research

## Status: closed (research complete)

## Obiettivo iniziale
Identificare inefficienze sfruttabili sui mercati BTC 15-min Polymarket per
un trader retail con €1000 di capitale.

## Tesi testate (e risultati)

1. **Trend follow sub-5min** — falsificata a priori per microstruttura
   (spread + adverse selection vs market maker HFT).
2. **Chainlink oracle lag** — falsificata: Polymarket usa Chainlink Data
   Streams (sub-secondo), non l'aggregator on-chain.
3. **Bias di calibrazione multi-horizon** — falsificata su 60 giorni di
   storia (5723 mercati, 7 orizzonti, stratificazione per regime vol/dir):
   9 bucket significativi su 420 test, sotto le 21 attese per puro caso;
   nessuno sopravvive a Bonferroni.

## Metodologia

- **Data collection**: polyrec dashboard (Binance WS + Polymarket RTDS +
  Chainlink on-chain aggregator) → CSV log.
- **Historical analysis**: Gamma API + CLOB prices-history + Binance klines
  → multi-horizon calibration con symmetric reflection.
- **Statistical validation**: binomial test per bucket + Bonferroni/FDR per
  multiple testing.

## Conclusione

Il mercato BTC 15-min Polymarket è statisticamente efficiente sugli
orizzonti operativamente rilevanti (T-1m, T-2m, T-5m). Nessun edge
retail-sfruttabile identificato.

## Stack tecnico (riusabile)

- WS aggregator multi-source (Binance, Polymarket CLOB, Chainlink RTDS).
- CSV logger con 70+ indicatori derivati.
- Framework calibrazione storica con stratificazione.

## Lezioni




## Credits

Fork di [github.com/txbabaxyz/polyrec](https://github.com/txbabaxyz/polyrec).
