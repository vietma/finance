select * from finance.stockquotes where last_trade > 0.5 order by change_in_percent desc limit 20;
select * from finance.crossover where crossover_status = 'True' order by change_in_percent desc;

select * from finance.stockquotes where symbol in ('ANZ.AX', 'WOR.AX', 'MPL.AX', 'RRL.AX', 'WBC.AX', 'RIO.AX', 'BHP.AX', 'TLS.AX', 'CTX.AX', 'NCM.AX', 'BAL.AX', 'YOW.AX', 'BKN.AX', 'BRN.AX', 'PRR.AX', 'LYC.AX', 'KNL.AX', 'MND.AX', 'LNG.AX', 'SLR.AX', 'YOW.AX', 'ODN.AX', 'KBL.AX');
