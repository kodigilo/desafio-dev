package com.example.cnab.util;

import com.example.cnab.domain.CnabTxt;
import com.example.cnab.enums.TipoTransacaoEnum;
import lombok.extern.slf4j.Slf4j;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class CnabUtil {

    public static List<CnabTxt> from(String conteudo){
        //guarda a lista final com os dados normalizados
        List<CnabTxt> result =  new ArrayList<>();
        //monta uma lista de linhas dos arquivos
        List<String> list = List.of(conteudo.split("\n"));
        //uma lista com todas as propriedades da classe
        var fields = CnabTxt.class.getDeclaredFields();

        list.forEach(s -> {

            if (s.length() < 70) throw new RuntimeException("Linha com menos de 80 caracteres.");

            CnabTxt linha = new CnabTxt();
            Arrays.stream(fields).forEach(field -> {
                //obtem as annotations
                var annotation = (CnabColuna) field.getAnnotation(CnabColuna.class);
                if (annotation == null) return;

                var start = annotation.start();
                var end = annotation.end();
                var coluna = field.getName();

                try{
                    Field variableName = linha.getClass().getDeclaredField(coluna);
                    variableName.setAccessible(true);

                    start = (start-1);
                    end = end > s.length() ? s.length() : end;

                    if(variableName.getType().isAssignableFrom(Integer.class)){
                        variableName.set(linha, Integer.valueOf(s.substring(start,end)));
                    }else if(variableName.getType().isAssignableFrom(TipoTransacaoEnum.class)){
                        variableName.set(linha, TipoTransacaoEnum.byInt(Integer.valueOf(s.substring(start,end))));
                    }else if(variableName.getType().isAssignableFrom(BigDecimal.class)){
                        variableName.set(linha, new BigDecimal(s.substring(start,end)));
                    }else if(variableName.getType().isAssignableFrom(String.class)){
                        variableName.set(linha, String.valueOf(s.substring(start,end)).trim());
                    }else if(variableName.getType().isAssignableFrom(Date.class)){
                        var format = new SimpleDateFormat("yyyyMMdd");
                        variableName.set(linha, format.parse(s.substring(start,end)));
                    }else if(variableName.getType().isAssignableFrom(LocalTime.class)){
                        LocalTime localTime = LocalTime.parse(s.substring(start,end), DateTimeFormatter.ofPattern("HHmmss"));
                        variableName.set(linha, localTime);
                    }
                }catch (Exception e){
                    log.error("Erro no parse do Cnab");
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            });
            result.add(linha);
        });
        return result;
    }

}
