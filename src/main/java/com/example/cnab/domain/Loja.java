package com.example.cnab.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@AllArgsConstructor
public class Loja {
    private String loja;
    private BigDecimal saldo = BigDecimal.ZERO;
    private List<CnabTxt> transacoes;

    public Loja(String loja) {
        this.loja = loja;
    }
}
