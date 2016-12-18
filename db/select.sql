select * from finance.stockquotes where last_trade > 0.5 order by change_in_percent desc limit 20;

select * from finance.crossover where last_trade > 0.1 and crossover_status = 'True' order by change_in_percent desc;

select * from finance.stockquotes where symbol in ('ANZ.AX', 'ALU.AX', 'SSM.AX', 'FRM.AX', 'WBC.AX', 'RIO.AX', 'BHP.AX', 'TLS.AX', 'CTX.AX', 'NCM.AX', 'BAL.AX', 'YOW.AX', 'BKN.AX', 'BRN.AX', 'PRR.AX', 'LYC.AX', 'KNL.AX', 'MND.AX', 'LNG.AX', 'SLR.AX', 'YOW.AX', 'ODN.AX', 'KBL.AX', 'OEL.AX', 'MQG.AX', 'FMG.AX', 'BHP.AX', 'SAR.AX', 'RSG.AX', 'KDR.AX', 'KBL.AX', 'MJP.AX', 'SEA.AX', 'OEL.AX', 'WOW.AX', 'RKN.AX');

select * from finance.stockquotes where symbol in ('NCM.AX','SAR.AX','SLR.AX','RRL.AX','NST.AX','OGC.AX','MML.AX','SBM.AX','RSG.AX','PRU.AX','AQG.AX');

select * from finance.stockquotes where symbol in ('IGL.AX','VAH.AX','COH.AX','DMP.AX','SOP.AX','RKN.AX','TMM.AX','BOL.AX','BFG.AX','MFG.AX','NCK.AX','BKN.AX','TLS.AX','GMG.AX','GXL.AX','GEM.AX','BNO.AX','CAT.AX','DWS.AX','CZZ.AX','SGQ.AX','MRN.AX','EGG.AX','OZL.AX','AGL.AX','CBA.AX','ACR.AX','FXJ.AX','IFL.AX','REA.AX','TCL.AX','MMS.AX','BBL.AX','RNY.AX','JRL.AX','VTM.AX','BEN.AX','PVE.AX','LEP.AX','BWP.AX','GMF.AX','DOW.AX','MBE.AX','SUN.AX','FFI.AX','TAH.AX') order by change_in_percent desc;

select * from finance.stockquotes where symbol in ('OTH.AX','ABV.AX','RES.AX','EVT.AX','FBR.AX','MIN.AX','PRO.AX','SHM.AX','RBX.AX','ICS.AX','GOZ.AX','GWA.AX','APN.AX','WSA.AX','SFR.AX','BRG.AX','PNV.AX','ACG.AX','RIS.AX','IFM.AX','IRI.AX','4DS.AX','SMD.AX','VSC.AX','CCL.AX','FFT.AX','DTL.AX','SOM.AX','GFY.AX','ITQ.AX','PSZ.AX','CNI.AX','SRS.AX','PBP.AX','NAN.AX','HFA.AX','TOX.AX','AVJ.AX','SIO.AX','RCR.AX','ABP.AX','CWY.AX','FMG.AX','SKI.AX','RIS.AX','CBC.AX','GLB.AX','IOG.AX','PTB.AX','OFW.AX','KME.AX','SOR.AX','FSA.AX','TYK.AX','CYP.AX','URF.AX','BPA.AX','UUL.AX','SHV.AX','GNG.AX','BAL.AX','CAB.AX','BGL.AX','PSQ.AX','LGD.AX','PNC.AX','XRF.AX','SKF.AX','PME.AX','GMY.AX','EZA.AX','MYS.AX','EPW.AX','MVP.AX','FTT.AX','TZN.AX','CAF.AX','PAY.AX','SPL.AX','JIN.AX','ANG.AX','RDH.AX','IDR.AX','ORG.AX','BLX.AX','SLX.AX','ISD.AX','NHF.AX','CAA.AX','CTX.AX','PBG.AX','HSO.AX','OSH.AX','BKL.AX','MMS.AX','MND.AX','QHL.AX','PRT.AX','SXY.AX','SCG.AX','SMX.AX','WFD.AX','SRX.AX','QAN.AX','QUB.AX','KSC.AX','JHC.AX','APO.AX','GDI.AX','SRG.AX','ANP.AX','AMO.AX','EXU.AX','ASN.AX','SIV.AX','EML.AX','WRR.AX','PPP.AX','CLT.AX','TDL.AX','EMB.AX','GHC.AX','MLD.AX','BLA.AX','CGL.AX','CDA.AX','OCL.AX','PFL.AX','APD.AX','PTL.AX','HIL.AX','MCP.AX','HOM.AX','AOA.AX','NEA.AX','DGH.AX','NDO.AX','TIA.AX','AYG.AX','AER.AX','KBU.AX','AKG.AX','RHP.AX','SBM.AX','PGH.AX','WOR.AX','PDN.AX','BLD.AX','AWC.AX','VTG.AX','APZ.AX','SIQ.AX','LNK.AX','HSN.AX','WES.AX','WBT.AX','OPG.AX','ILU.AX','PTM.AX','S32.AX','FRM.AX') order by change_in_percent desc;

select * from finance.stockquotes where last_trade_date >= (CURDATE() - INTERVAL 1 DAY) and change_in_percent > 0 and last_trade > 0.1 and volume > average_daily_volume and last_trade*volume > 2000000 order by last_trade_date desc, change_in_percent desc;

select symbol, change_in_percent, last_trade from finance.stockquotes where last_trade_date >= (CURDATE() - INTERVAL 1 DAY) and change_in_percent > 0 and last_trade > 0.1 and volume > average_daily_volume and last_trade*volume > 2000000 order by last_trade_date desc, change_in_percent desc;

select * from finance.historicalprices;





