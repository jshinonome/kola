\S 42

rowText:getenv `KOLA_Q_ROWS;
n:$[0=count rowText;10000;"J"$rowText];
if[(null n)|n<1|n>2000000; -2 "invalid KOLA_Q_ROWS: ",rowText; exit 1];

base:([]
  sym:n?`AAPL`MSFT`GOOG`AMZN;
  time:2024.01.02D00:00:00.000000000 + 1000000 * "n"$til n;
  volume:n?1000;
  cond:n#enlist "aaa");

tradeColumns:`$("ask";"bid") cross string til 5;
trade:![base;();0b;tradeColumns!(count tradeColumns)#enlist(?;n;1.0)];

wideColumns:`$("ask";"bid") cross string til 30;
wide:![base;();0b;wideColumns!(count wideColumns)#enlist(?;n;1.0)];

depth:([]
  sym:n?`AAPL`MSFT`GOOG`AMZN;
  time:2024.01.02D00:00:00.000000000 + 1000000 * "n"$til n;
  volume:n?1000;
  ask:n#enlist 5?1.0;
  bid:n#enlist 5?1.0);

.kola.rows:n;
.kola.seed:42;
.kola.ready:1b;
