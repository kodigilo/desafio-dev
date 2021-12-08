<template>
    <div>
        <table>
            <tr>
                <th>Data</th>
                <th>Cartão</th>
                <th>CPF</th>
                <th>Tipo</th>
                <th>Valor</th>
                <th>Loja</th>
                <th>Dono</th>
            </tr>
            <tr v-for="transacao in transacoes" :key="transacao.id">
                <td>{{ transacao.dataHorario | f_date }}</td>
                <td>{{ transacao.cartao }}</td>
                <td>{{ transacao.cpf }}</td>
                <td>{{ transacao.tipoEnum }}</td>
                <td>{{ transacao.sinal }}{{ transacao.valor | currency }}</td>
                <td>{{ transacao.loja }}</td>
                <td>{{ transacao.donoLoja }}</td>
            </tr>
        </table>
    </div>
</template>

<script>
export default {
    name: "Transacao",
    props : ['transacoes']
}
</script>

<style scoped>

</style>