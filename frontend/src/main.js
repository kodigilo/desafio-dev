import Vue from 'vue'
import App from './App.vue'
import axios from "axios";
import Vue2Filters from 'vue2-filters'
import moment from 'moment'

Vue.use(Vue2Filters, {
  currency: {
    symbol: 'R$ ',
    decimalDigits: 2,
    thousandsSeparator: '.',
    decimalSeparator: ',',
    symbolOnLeft: true,
    spaceBetweenAmountAndSymbol: false,
    showPlusSign: false
  },
})

Vue.filter('f_date', function(value) {
  if (value) {
    return moment(String(value)).format('DD/MM/YYYY')
  }
  return '';
})



axios.defaults.baseURL = process.env.VUE_APP_API_BASE_URL;

Vue.config.productionTip = false

new Vue({
  render: h => h(App),
  axios
}).$mount('#app')
