package com.example.cnab.repository;

public interface CnabRepository {
    String insert = "insert into transacoes (id, tipo, data, valor, cpf, cartao, hora, dono_loja, loja) VALUES (:id, :tipo, :data, :valor, :cpf, :cartao, :hora, :donoLoja, :loja) on duplicate key update id = id";
}
