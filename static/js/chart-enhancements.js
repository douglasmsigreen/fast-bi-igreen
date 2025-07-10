// Funções auxiliares para melhorias visuais dos gráficos
function createGradientBackground(ctx, chartArea, colors) {
    const gradient = ctx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
    gradient.addColorStop(0, colors.start);
    gradient.addColorStop(0.5, colors.middle);
    gradient.addColorStop(1, colors.end);
    return gradient;
}

function formatTooltipValue(value, type = 'number') {
    if (type === 'percentage') {
        return value.toFixed(1) + '%';
    }
    return new Intl.NumberFormat('pt-BR').format(value);
}

// Configurações de animação padrão
const defaultAnimationConfig = {
    enabled: true,
    easing: 'easeinout',
    speed: 1500,
    animateGradually: {
        enabled: true,
        delay: 200
    }
};