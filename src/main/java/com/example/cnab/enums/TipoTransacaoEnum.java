package com.example.cnab.enums;

import lombok.Getter;

public enum TipoTransacaoEnum {

    DEBITO(1, true, "+", "Débito"),
    BOLETO(2, false, "-", "Boleto"),
    FINANCIAMENTO(3, false, "-", "Financiamento"),
    CREDITO(4, true, "+", "Crédito"),
    RECEBIMENTO_EMPRESTIMO(5, true, "+", "Recebimento Empréstimo"),
    VENDAS(6, true, "+", "Vendas"),
    RECEBIMENTO_TED(7, true, "+", "Recebimento TED"),
    RECEBIMENTO_DOC(8, true, "+", "Recebimento DOC"),
    ALUGUEL(9, false, "-", "Aluguel");

    @Getter
    private Integer tipo;
    @Getter
    private boolean natureza;
    @Getter
    private String sinal;
    @Getter
    private String label;


    TipoTransacaoEnum(Integer tipo, boolean natureza, String sinal, String label) {
        this.tipo = tipo;
        this.natureza = natureza;
        this.sinal = sinal;
        this.label = label;
    }

    public static TipoTransacaoEnum byInt(int x) {
        switch(x) {
            case 1:
                return TipoTransacaoEnum.DEBITO;
            case 2:
                return TipoTransacaoEnum.BOLETO;
            case 3:
                return TipoTransacaoEnum.FINANCIAMENTO;
            case 4:
                return TipoTransacaoEnum.CREDITO;
            case 5:
                return TipoTransacaoEnum.RECEBIMENTO_EMPRESTIMO;
            case 6:
                return TipoTransacaoEnum.VENDAS;
            case 7:
                return TipoTransacaoEnum.RECEBIMENTO_TED;
            case 8:
                return TipoTransacaoEnum.RECEBIMENTO_DOC;
            case 9:
                return TipoTransacaoEnum.ALUGUEL;
        }
        return null;
    }
}
