<template>
    <div id="app">

        <div class="container">

            <h1>CNAB Desafio</h1>

            <div>
                <label>Upload do CNAB</label>
                <div>
                    <input type="file" name="file" id="file" ref="file" v-on:change="loadFile()">
                    <button @click="upload">Enviar</button>
                    <div v-if="result==2" class="msg msg-erro">{{ msg }}</div>
                    <div v-if="result==1" class="msg msg-sucesso">{{ msg }}</div>
                </div>
            </div>

            <br>
            <hr>
            <br>

            <div class="linha">
                <div class="coluna">
                    <h2>Transações</h2>
                    <div v-if="transacoesMsg">
                        <div class="msg msg-erro">{{ transacoesMsg }}</div>
                    </div>
                    <Transacao :transacoes="transacoes"></Transacao>
                </div>
                <div class="coluna">
                    <h2>Lojas</h2>
                    <div v-if="lojasMsg">
                        <div class="msg msg-erro">{{ lojasMsg }}</div>
                    </div>
                    <table>
                        <tr>
                            <th>Loja</th>
                            <th>Saldo</th>
                            <th>Transações</th>
                        </tr>
                        <tr v-for="loja in lojas" :key="loja.loja">
                            <td>{{ loja.loja }}</td>
                            <td>{{ loja.saldo | currency }}</td>
                            <td>
                                <TransacaoLoja :transacoes="loja.transacoes"></TransacaoLoja>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>

        </div>


    </div>
</template>

<script>
import axios from "axios";
import Transacao from "@/components/Transacao";
import TransacaoLoja from "@/components/TransacaoLoja";
export default {
    name: 'App',
    components: {
        Transacao,
        TransacaoLoja
    },
    data() {
        return {
            result: 0,
            msg: '',
            file: '',
            transacoes: [],
            transacoesMsg: '',
            lojas: [],
            lojasMsg: '',
        }
    },
    mounted() {
        this.getLojas();
        this.getTransacoes();
    },
    methods: {
        getTransacoes: function () {
            axios.get('v1/paginacao').then((response) => {
                this.transacoes = response.data.content
                console.log(this.transacoes)
            }).catch((erro) => {
                console.log(erro)
                this.transacoesMsg = "Erro ao carregar as transações"
            })
        },
        getLojas: function () {
            axios.get('v1/lojas').then((response) => {
                this.lojas = response.data
                console.log(this.lojas)
            }).catch((erro) => {
                console.log(erro)
                this.lojasMsg = "Erro ao carregar as lojas"
            })
        },
        upload: function () {
            let formData = new FormData();
            formData.append('file', this.file);
            axios.post('v1', formData, {
                headers: {
                    'Content-Type': 'multipart/form-data'
                }
            }).then((response) => {
                this.result = 1
                this.msg = response.data;
                console.log(response)
            }).catch((erro) => {
                this.result = 2
                console.log(erro)
                console.log({erro})
            })
        },
        loadFile: function () {
            this.file = this.$refs.file.files[0];
        }
    }
}
</script>

<style>
#app {
    font-family: Avenir, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: #2c3e50;
    margin-top: 60px;
}

.container {
    width: 1000px;
    margin: 0 auto;
}

input {
    border: 2px solid #ccc;
    padding: 10px;
    margin: 10px 0;
    border-radius: 10px 0 0 10px;
    border-right: none;
}

label {
    font-weight: bold;
}

button {
    background: #0db47a;
    color: #fff;
    border: none;
    padding: 15px;
    border-radius: 0 10px 10px 0;
    text-transform: uppercase;
    font-weight: bold;
}

.msg {
    font-weight: bold;
    font-style: italic;
}

.msg-erro {
    color: red
}

.msg-sucesso {
    color: #0db47a
}

hr {
    border: 1px solid #cccc;
    outline: none;
    margin: 10px 0;
}





table {
    width: 100%;
    text-align: left;
    vertical-align: top;
}

table td, table th {
    padding: 3px;
    vertical-align: top;
}
</style>
