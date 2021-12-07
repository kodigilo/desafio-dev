package com.example.cnab.controller;

import com.example.cnab.domain.CnabTxt;
import com.example.cnab.domain.Loja;
import com.example.cnab.service.CnabService;
import com.example.cnab.util.CnabUtil;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
@AllArgsConstructor
@RequestMapping({"/v1"})
public class IndexController {

    CnabService service;


    @ApiOperation(value = "Traz todos os registros com paginação" )
    @GetMapping("paginacao")
    public ResponseEntity<?> paginacao(
            @RequestParam(value = "limit", required = false, defaultValue = "20") int limit,
            @RequestParam(value = "page", required = false, defaultValue = "0") int page,
            @RequestParam(value = "orderby", required = false, defaultValue = "id") String orderby,
            @RequestParam(value = "order", required = false, defaultValue = "DESC") String order
    ){
        try {
            PageRequest pageRequest = PageRequest.of(page, limit, Sort.by(Sort.Direction.valueOf(order), orderby));
            return ResponseEntity.ok().body(new PageImpl<CnabTxt>(service.paginate(pageRequest),pageRequest, service.total()));
        }catch (Exception e){
            return ResponseEntity.badRequest().body("Erro ao paginar dados: " + e.getMessage());
        }
    }

    @ApiOperation(value = "Traz todos os registros com paginação" )
    @GetMapping("lojas")
    public ResponseEntity<?> lojas(){
        try {
            var transacoes = service.all();
            var lojas = transacoes.stream().map(x-> new Loja(x.getLoja()) ).distinct().collect(Collectors.toList());
            lojas.forEach(loja -> {
                loja.setTransacoes(transacoes.stream().sorted((d1,d2) -> d1.getDataHorario().compareTo(d2.getDataHorario())).filter(x->x.getLoja().equals(loja.getLoja())).collect(Collectors.toList()));
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
            });
            return ResponseEntity.ok().body(lojas);
        }catch (Exception e){
            return ResponseEntity.badRequest().body("Erro ao paginar lojas: " + e.getMessage());
        }
    }

    @ApiOperation(value = "Processa um CNAB", notes = "Converte um CNAB para JSON")
    @PostMapping
    public ResponseEntity<?> processador(@RequestParam("file") MultipartFile file) {
        try {
            List<CnabTxt> list = CnabUtil.from(new String(file.getBytes(), StandardCharsets.UTF_8));
            service.insert(list);
            return ResponseEntity.ok().body("Arquivo processado com sucesso");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Erro ao processar arquivo: " + e.getMessage());
        }
    }

}
