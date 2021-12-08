<template>
    <div>
        <table>
            <tr>
                <th>Cartão</th>
                <th>CPF</th>
                <th>Valor</th>
            </tr>
            <tr v-for="transacao in transacoes" :key="transacao.id">
                <td>{{ transacao.cartao }}</td>
                <td>{{ transacao.cpf }}</td>
                <td>{{ transacao.valor | currency }}</td>
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