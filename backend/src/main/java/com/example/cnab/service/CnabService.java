package com.example.cnab.service;


import com.example.cnab.domain.CnabTxt;
import com.example.cnab.domain.Loja;
import com.example.cnab.repository.CnabRepository;
import com.example.cnab.util.Util;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class CnabService {

    JdbcTemplate jdbcTemplate;

    private NamedParameterJdbcTemplate getTemplate() {
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
        return template;
    }

    public boolean insert(List<CnabTxt> list) {

        try {
            SqlParameterSource[] parametros = new SqlParameterSource[list.size()];
            int contador = 0;

            for (CnabTxt linha : list) {
                //ID com MD5 evita duplicidade no banco de dados
                linha.setId(Util.MD5(new Gson().toJson(linha)));
                parametros[contador] = new BeanPropertySqlParameterSource(linha);
                contador++;
            }

            getTemplate().batchUpdate(CnabRepository.insert, parametros);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public List<CnabTxt> all() {
        SqlParameterSource params = new MapSqlParameterSource();
        return getTemplate().query("select * from transacoes ", params, BeanPropertyRowMapper.newInstance(CnabTxt.class));
    }

    public List<CnabTxt> paginate(PageRequest pageRequest) {
        SqlParameterSource params = new MapSqlParameterSource();
        return getTemplate().query(limitOffset("select * from transacoes ", pageRequest), params, BeanPropertyRowMapper.newInstance(CnabTxt.class));
    }

    public Integer total() {
        SqlParameterSource params = new MapSqlParameterSource();
        return getTemplate().queryForObject("select count(*) from transacoes ", params,Integer.class);
    }

    private String limitOffset(String sql, Pageable pageRequest) {
        int offset = pageRequest.getPageNumber() * pageRequest.getPageSize();
        int limit = pageRequest.getPageSize();
        return sql + " LIMIT " + offset + "," + limit;
    }


    public void calculaSaldo(Loja loja,List<CnabTxt> transacoes){
        loja.setTransacoes(transacoes.stream().sorted((d1,d2) -> d1.getDataHorario().compareTo(d2.getDataHorario())).filter(x->x.getLoja().equals(loja.getLoja())).collect(Collectors.toList()));
    }
    public void calculaSaldo(Loja loja){
        loja.getTransacoes().stream().sorted((d1,d2) -> d1.getDataHorario().compareTo(d2.getDataHorario())).forEach(cnabTxt -> {
            switch (cnabTxt.getTipoEnum().getSinal()){
                case "+" :
                    loja.setSaldo(BigDecimal.valueOf(loja.getSaldo().doubleValue() + cnabTxt.getValor().doubleValue()));
                    break;
                case "-" :
                    loja.setSaldo(BigDecimal.valueOf(loja.getSaldo().doubleValue() - cnabTxt.getValor().doubleValue()));
                    break;
            }
        });
    }

}
