const { assign } = require('xstate')

const discoImplementation = {
  actions: {
    incrementClick: assign({ clicks: context => context.clicks + 1 })
  },
  guards: {
    hasReachedLimit: (context) => context.clicks >= context.max_clicks,
  },
  services: {
    timer: (context, event) => send => {
      const interval = setInterval(() => {
        send('SEND_STATES')
      }, 1000)
      return () => clearInterval(interval)
    }
  }
}

module.exports = discoImplementation