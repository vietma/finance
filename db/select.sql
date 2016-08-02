select * from finance.stockquotes where last_trade > 0.5 order by change_in_percent desc limit 20;

select * from finance.crossover where last_trade > 0.1 and crossover_status = 'True' order by change_in_percent desc;

select * from finance.stockquotes where symbol in ('ANZ.AX', 'WOR.AX', 'MPL.AX', 'RRL.AX', 'WBC.AX', 'RIO.AX', 'BHP.AX', 'TLS.AX', 'CTX.AX', 'NCM.AX', 'BAL.AX', 'YOW.AX', 'BKN.AX', 'BRN.AX', 'PRR.AX', 'LYC.AX', 'KNL.AX', 'MND.AX', 'LNG.AX', 'SLR.AX', 'YOW.AX', 'ODN.AX', 'KBL.AX', 'OEL.AX', 'MQG.AX', 'FMG.AX', 'BHP.AX', 'SAR.AX', 'RSG.AX', 'KDR.AX', 'KBL.AX', 'MJP.AX', 'SEA.AX', 'OEL.AX', 'WOW.AX', 'RKN.AX');

select * from finance.stockquotes where symbol in ('NCM.AX','SAR.AX','SLR.AX','RRL.AX','NST.AX','OGC.AX','MML.AX','SBM.AX','RSG.AX','PRU.AX','AQG.AX');

select * from finance.stockquotes where last_trade_date >= (CURDATE() - INTERVAL 1 DAY) and change_in_percent > 0 and last_trade > 0.1 and volume > average_daily_volume and last_trade*volume > 2000000 order by last_trade_date desc, change_in_percent desc;



