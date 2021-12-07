package com.example.cnab.controller;

import com.example.cnab.domain.CnabTxt;
import com.example.cnab.domain.Loja;
import com.example.cnab.service.CnabService;
import com.example.cnab.util.CnabUtil;
import io.github.glytching.junit.extension.random.Random;
import io.github.glytching.junit.extension.random.RandomBeansExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith({MockitoExtension.class, RandomBeansExtension.class})
@Slf4j
class IndexControllerTest {

    @Mock CnabService cnabService;
    @InjectMocks IndexController indexController;
    @Random String conteudoErro;
    private String conteudo = "3201903010000014200096206760174753****3153153453JOÃO MACEDO   BAR DO JOÃO       \n" +
            "5201903010000013200556418150633123****7687145607MARIA JOSEFINALOJA DO Ó - MATRIZ\n" +
            "3201903010000012200845152540736777****1313172712MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "2201903010000011200096206760173648****0099234234JOÃO MACEDO   BAR DO JOÃO       \n" +
            "1201903010000015200096206760171234****7890233000JOÃO MACEDO   BAR DO JOÃO       \n" +
            "2201903010000010700845152540738723****9987123333MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "2201903010000050200845152540738473****1231231233MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "3201903010000060200232702980566777****1313172712JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "1201903010000020000556418150631234****3324090002MARIA JOSEFINALOJA DO Ó - MATRIZ\n" +
            "5201903010000080200845152540733123****7687145607MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "2201903010000010200232702980568473****1231231233JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "3201903010000610200232702980566777****1313172712JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "4201903010000015232556418150631234****6678100000MARIA JOSEFINALOJA DO Ó - FILIAL\n" +
            "8201903010000010203845152540732344****1222123222MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "3201903010000010300232702980566777****1313172712JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "9201903010000010200556418150636228****9090000000MARIA JOSEFINALOJA DO Ó - MATRIZ\n" +
            "4201906010000050617845152540731234****2231100000MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "2201903010000010900232702980568723****9987123333JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "8201903010000000200845152540732344****1222123222MARCOS PEREIRAMERCADO DA AVENIDA\n" +
            "2201903010000000500232702980567677****8778141808JOSÉ COSTA    MERCEARIA 3 IRMÃOS\n" +
            "3201903010000019200845152540736777****1313172712MARCOS PEREIRAMERCADO DA AVENIDA";

    @Test
    void paginacao() {
        PageRequest pageRequest = PageRequest.of(0, 20, Sort.by(Sort.Direction.valueOf("DESC"), "id"));
        var lista = CnabUtil.from(conteudo);
        Mockito.when(cnabService.paginate(pageRequest)).thenReturn(lista);
        Mockito.when(cnabService.total()).thenReturn(21);
        var response = indexController.paginacao(20,0,"id","DESC");
        assertEquals(HttpStatus.OK,response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void paginacaoErro() {
        PageRequest pageRequest = PageRequest.of(0, 20, Sort.by(Sort.Direction.valueOf("DESC"), "id"));
        var lista = CnabUtil.from(conteudo);
        Mockito.when(cnabService.paginate(pageRequest)).thenReturn(lista);
        Mockito.when(cnabService.total()).thenReturn(21);
        var response = indexController.paginacao(20,0,"xxxx","DESC");
        assertEquals(HttpStatus.BAD_REQUEST,response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void lojas() {
        Mockito.when(cnabService.all()).thenReturn(CnabUtil.from(conteudo));
        var response = indexController.lojas();
        assertEquals(HttpStatus.OK,response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void processador() {
        String name = "file.txt";
        String originalFileName = "file.txt";
        String contentType = "text/plain";
        Mockito.when(cnabService.insert(CnabUtil.from(conteudo))).thenReturn(true);
        var response = indexController.processador(new MockMultipartFile(name,originalFileName,contentType,conteudo.getBytes()));
        assertEquals(HttpStatus.OK,response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void processadorComErro() {
        String name = "file.txt";
        String originalFileName = "file.txt";
        String contentType = "text/plain";
        var response = indexController.processador(new MockMultipartFile(name,originalFileName,contentType,conteudoErro.getBytes()));
        assertEquals(HttpStatus.BAD_REQUEST,response.getStatusCode());
        assertNotNull(response.getBody());
    }
}