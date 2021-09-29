Objetivo
    O objetivo do Scientist é auxiliar a busca de melhorias nas estratégias do prophet.

Por que?
    1. O prophet hoje lida com características muito específicas para manter estabilidade entre backtest e a operação real. Esses desafios tornam a capacidade de fazer backtests e tentativas muito restritas e dificultam a busca de novas moedas, novos rankings, novas entradas e saídas. 

    2. A estratégia do Prophet não é eficiente em momentos de altas no mercado que poderiam impulsionar muito os ganhos dos investidores para momentos de perda em trades sem sucesso, que acontecem com frequencia regular, apesar de não resultar em perdas de capital num período maior (10 a 30 dias).

Como?

# Feature Importance 

## symbols-aug-1d
TRADES          MID_BB      BB      VOLUME      OTHER           MM              STD
trades_d0       mid_bb99    bb5     d_vol_99    d0              close_mm5       std21
d_trades_200    mid_bb200   bb7     d_vol_200   dp              close_mm99      std25
d_trades_99     mid_bb5     bb51    d_vol_51    q_volume_d0     close_mm15      std51
d_trades_25     mid_bb10    bb99    d_vol_21    trades_d0       close_mm25      std20
                mid_bb51    bb10    d_vol_25                    close_mm200     std99