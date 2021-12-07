package com.example.cnab.domain;

import com.example.cnab.enums.TipoTransacaoEnum;
import com.example.cnab.util.CnabColuna;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.Date;

@Data
public class CnabTxt {
    private String id;

    @CnabColuna(start = 1, end = 1)
    private TipoTransacaoEnum tipoEnum;
    @CnabColuna(start = 1, end = 1)
    private Integer tipo;
    @CnabColuna(start = 2, end = 9)
    private Date data;
    @CnabColuna(start = 10, end = 19)
    private BigDecimal valor;
    @CnabColuna(start = 20, end = 30)
    private String cpf;
    @CnabColuna(start = 31, end = 42)
    private String cartao;
    @CnabColuna(start = 43, end = 48)
    private LocalTime hora;
    @CnabColuna(start = 49, end = 62)
    private String donoLoja;
    @CnabColuna(start = 63, end = 81)
    private String loja;



    public void setTipo(Integer tipo) {
        this.tipo = tipo;
        tipoEnum = TipoTransacaoEnum.byInt(this.tipo);
    }

    public Timestamp getDataHorario(){
        try{
            var format = new SimpleDateFormat("yyyy-MM-dd");
            var horax = hora.toString();
            if(horax.length()<=5) horax += ":"+hora.getSecond();
            var string = (format.format(data)+" "+horax);
            var datax = Timestamp.valueOf(string);
            return datax;
        }catch (Exception e){
            return new Timestamp(data.getTime());
        }
    }

}
