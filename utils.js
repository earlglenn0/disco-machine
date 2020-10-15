const get = require('lodash/get')

const formatState = state => ({
  isOn: Boolean(get(state, 'on')),
  speed: get(state, 'on.speed', 'low'),
  color: get(state, 'on.color', 'steady'),
  lightMode: get(state, 'on.lightMode', 'steady')
})

module.exports = {
  formatState
}