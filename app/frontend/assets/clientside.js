window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        resize_charts: function(n_intervals) {
            if (window.innerWidth < 768) {
                // Mobile layout adjustments
                document.querySelectorAll('.chart-container').forEach(function(container) {
                    container.style.height = '300px';
                });
            } else {
                // Desktop layout
                document.querySelectorAll('.chart-container').forEach(function(container) {
                    container.style.height = '400px';
                });
            }
            return window.innerWidth;
        }
    }
}); 