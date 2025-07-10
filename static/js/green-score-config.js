// Configurações centralizadas para a página Green Score
const GREEN_SCORE_CONFIG = {
    colors: {
        primary: '#00c93b',
        secondary: '#16a34a',
        success: '#10b981',
        warning: '#f59e0b',
        danger: '#ef4444',
        neutral: '#6b7280'
    },
    
    scoreRanges: {
        excellent: { min: 80, colors: ['#86efac', '#16a34a'] },
        good: { min: 60, colors: ['#fde047', '#f97316'] },
        fair: { min: 40, colors: ['#fdba74', '#dc2626'] },
        poor: { min: 0, colors: ['#fca5a5', '#b91c1c'] }
    },
    
    animations: {
        duration: 1500,
        easing: 'easeinout',
        delay: 200
    },
    
    chart: {
        height: 320,
        responsive: true,
        maintainAspectRatio: false
    }
};