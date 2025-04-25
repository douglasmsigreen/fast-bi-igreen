/** @type {import('tailwindcss').Config} */
module.exports = {
  // Conteúdo a ser escaneado pelo Tailwind:
  content: [
    "./templates/**/*.html", // << IMPORTANTE: Procura por todos os .html em 'templates' e subpastas
    "./static/js/**/*.js"    // << Opcional: Inclui JS se você adicionar classes Tailwind dinamicamente via JS
  ],

  // Tema (onde você pode adicionar suas cores, fontes, etc.)
  theme: {
    extend: {
      // Exemplo de como adicionar a cor 'igreen' que usamos nos exemplos anteriores
      colors: {
        'igreen': {
          light: '#34d399',    // Ex: Verde claro para hover ou fundos sutis
          DEFAULT: '#00B034', // A cor principal (usada com bg-igreen, text-igreen, border-igreen)
          dark: '#00962C',     // Ex: Verde escuro para texto ou elementos mais fortes
        },
        // Você pode adicionar outras cores aqui
        // 'gray': { ... } // Ex: Sobrescrever ou adicionar tons de cinza
      },
      // Exemplo de como definir a fonte 'Inter' como padrão (se não estiver usando via <link>)
      fontFamily: {
        sans: ['Inter', 'sans-serif'], // Define 'Inter' como a fonte sans-serif padrão
        // serif: ['Merriweather', 'serif'], // Exemplo se precisar de fonte serifada
      },
    },
  },

  // Plugins (ex: para estilização extra de formulários)
  plugins: [
     // Descomente a linha abaixo se quiser usar o plugin oficial de formulários do Tailwind
     // require('@tailwindcss/forms'),
  ],
}