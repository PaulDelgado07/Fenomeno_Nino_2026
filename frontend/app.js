// CONFIGURACIÓN E INICIALIZACIÓN DEL MAPA
const API_BASE = "/api";

const map = L.map('map', {
    zoomControl: true,
    attributionControl: false
}).setView([-2.175, -79.910], 12.5);

// Capa Base de OpenStreetMap (Estilo Oscuro CartoDB)
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19
}).addTo(map);

// CAPAS DE VECTORES (Estilos y Definiciones)
let sectorsLayer = null;
let riversLayer = null;
let demLayer = null;
let historyLayer = null;
let activeRouteLine = null;
let shelterMarkersGroup = L.layerGroup().addTo(map);

// Catálogo de Albergues
const alberguesCoords = {
    "Samanes": { lat: -2.1054, lon: -79.8950, name: "Polideportivo Samanes" },
    "Sauces": { lat: -2.1054, lon: -79.8950, name: "Polideportivo Samanes" },
    "Suburbio Oeste": { lat: -2.1850, lon: -79.8970, name: "Coliseo Abel Jiménez Parra" },
    "Isla Trinitaria": { lat: -2.2150, lon: -79.9000, name: "Coliseo Alta Trinitaria" }
};

// Datos estáticos en memoria para cachear el estado de riesgo recibido del API
let currentRiskData = {};

// 1. INICIALIZAR GRÁFICOS (ApexCharts)
const sstChartOptions = {
    chart: {
        type: 'line',
        height: 200,
        toolbar: { show: false },
        animations: { enabled: true, easing: 'linear', dynamicAnimation: { speed: 1000 } },
        background: 'transparent'
    },
    theme: { mode: 'dark' },
    stroke: { curve: 'smooth', width: 3 },
    colors: ['#0ea5e9'],
    series: [{ name: 'Temperatura SST', data: [] }],
    xaxis: { type: 'datetime', labels: { show: false } },
    yaxis: { title: { text: 'Temp (°C)' }, min: 25, max: 32 },
    grid: { borderColor: 'rgba(255,255,255,0.05)' }
};
const sstChart = new ApexCharts(document.querySelector("#sst-chart"), sstChartOptions);
sstChart.render();

const precipChartOptions = {
    chart: {
        type: 'bar',
        height: 200,
        toolbar: { show: false },
        background: 'transparent'
    },
    theme: { mode: 'dark' },
    colors: ['#38bdf8'],
    plotOptions: { bar: { borderRadius: 4, columnWidth: '55%' } },
    series: [{ name: 'Lluvia Actual', data: [0, 0, 0, 0] }],
    xaxis: { categories: ['Trinitaria', 'Suburbio', 'Sauces', 'Samanes'] },
    yaxis: { title: { text: 'Intensidad (mm/h)' }, max: 60 },
    grid: { borderColor: 'rgba(255,255,255,0.05)' }
};
const precipChart = new ApexCharts(document.querySelector("#precip-chart"), precipChartOptions);
precipChart.render();


// 2. FUNCIÓN DE RENDERIZADO DE RÍOS E HIDROGRAFÍA (Polilíneas Leaflet)
function drawHydrography() {
    const riverCoords = [
        // Río Daule (Norte-Centro)
        [[-2.080, -79.873], [-2.110, -79.880], [-2.140, -79.875], [-2.180, -79.871]],
        // Río Babahoyo (Este)
        [[-2.145, -79.810], [-2.155, -79.840], [-2.170, -79.865], [-2.180, -79.871]],
        // Río Guayas (Unión)
        [[-2.180, -79.871], [-2.210, -79.880], [-2.250, -79.890], [-2.290, -79.900]],
        // Estero Salado (Suroeste)
        [[-2.175, -79.932], [-2.195, -79.939], [-2.215, -79.928], [-2.240, -79.915], [-2.260, -79.905]]
    ];
    
    const lines = riverCoords.map(coords => L.polyline(coords, {
        color: '#1d4ed8',
        weight: 6,
        opacity: 0.65,
        lineCap: 'round',
        lineJoin: 'round'
    }));
    
    riversLayer = L.layerGroup(lines);
    if (document.getElementById('layer-rivers').checked) {
        riversLayer.addTo(map);
    }
}

// 3. CAPA TOPOGRÁFICA / DEM (Pintar buffers o áreas de cota muy baja)
function drawDEMLayer() {
    // Definimos círculos concéntricos en las cotas más bajas (Suburbio Oeste e Isla Trinitaria)
    const lowSpots = [
        { name: "Cota Crítica - Isla Trinitaria (1.8m)", coords: [-2.2360, -79.9100], radius: 1500 },
        { name: "Cota Baja - Suburbio Oeste (2.2m)", coords: [-2.1990, -79.9190], radius: 1800 },
        { name: "Cota Baja - Sauces (2.8m)", coords: [-2.1500, -79.9000], radius: 1200 }
    ];
    
    const circles = lowSpots.map(spot => L.circle(spot.coords, {
        radius: spot.radius,
        fillColor: '#6366f1',
        fillOpacity: 0.2,
        color: '#4f46e5',
        weight: 1,
        dashArray: '4, 4'
    }).bindPopup(`<strong>${spot.name}</strong>`));
    
    demLayer = L.layerGroup(circles);
    if (document.getElementById('layer-dem').checked) {
        demLayer.addTo(map);
    }
}

// 4. ZONAS HISTÓRICAMENTE INUNDABLES (Puntos calientes)
function drawHistoryLayer() {
    const historicalSpots = [
        { name: "Sauces 6 - Inundación recurrente por marea y lluvias", coords: [-2.143, -79.897] },
        { name: "Sauces 3 - Colapso de alcantarillado e influencia del Río Daule", coords: [-2.151, -79.894] },
        { name: "Av. Barcelona (Suburbio) - Desbordamiento Estero Salado", coords: [-2.194, -79.924] },
        { name: "Trinitaria Sur - Anegación severa en pleamar máxima", coords: [-2.242, -79.905] }
    ];
    
    const markers = historicalSpots.map(spot => L.circleMarker(spot.coords, {
        radius: 8,
        fillColor: '#a855f7',
        fillOpacity: 0.65,
        color: '#f3e8ff',
        weight: 1.5
    }).bindPopup(`<strong>Zona Histórica de Riesgo</strong><br>${spot.name}`));
    
    historyLayer = L.layerGroup(markers);
    if (document.getElementById('layer-history').checked) {
        historyLayer.addTo(map);
    }
}

// 5. OBTENER ESTILO DE RIESGO DE ACUERDO CON EL VALOR Y EL SWITCH COMBINADO
function getRiskStyle(feature) {
    const zoneName = feature.properties.name;
    const data = currentRiskData[zoneName];
    
    if (!data) {
        return { fillColor: '#64748b', fillOpacity: 0.15, color: '#475569', weight: 1.5 };
    }
    
    const isCombinedEnabled = document.getElementById('layer-combined').checked;
    
    // Si desactivan el escenario combinado, recalculamos el nivel de riesgo restando el bonus.
    let riskIndex = data.risk_index;
    let riskLevel = data.risk_level;
    
    if (!isCombinedEnabled) {
        // En Spark, el combined_bonus suma 10 cuando precipitación >= 25 y marea >= 2.5
        const hasBonus = data.precipitation_mm_h >= 25 && data.tide_m >= 2.5;
        if (hasBonus) {
            riskIndex -= 10;
            if (riskIndex >= 70) riskLevel = "Crítico";
            else if (riskIndex >= 50) riskLevel = "Alto";
            else if (riskIndex >= 30) riskLevel = "Medio";
            else riskLevel = "Bajo";
        }
    }
    
    let color = '#10b981'; // Bajo
    if (riskLevel === 'Medio') color = '#f59e0b';
    else if (riskLevel === 'Alto') color = '#ea580c';
    else if (riskLevel === 'Crítico') color = '#ef4444';
    
    return {
        fillColor: color,
        fillOpacity: isCombinedEnabled && riskLevel === 'Crítico' ? 0.65 : 0.45,
        color: color,
        weight: isCombinedEnabled && riskLevel === 'Crítico' ? 3 : 1.5,
        dashArray: isCombinedEnabled && riskLevel === 'Crítico' ? '' : '3'
    };
}

// 6. CARGAR Y PINTAR SECTORES GEOJSON
function loadSectorsGeoJSON() {
    fetch('data/gye_sectors.geojson')
        .then(res => res.json())
        .then(geoJsonData => {
            if (sectorsLayer) {
                map.removeLayer(sectorsLayer);
            }
            
            sectorsLayer = L.geoJSON(geoJsonData, {
                style: getRiskStyle,
                onEachFeature: function (feature, layer) {
                    layer.on({
                        mouseover: function (e) {
                            const l = e.target;
                            l.setStyle({ fillOpacity: 0.75, weight: 2.5 });
                        },
                        mouseout: function (e) {
                            sectorsLayer.resetStyle(e.target);
                        },
                        click: function (e) {
                            const zoneName = feature.properties.name;
                            document.getElementById('select-zone-evac').value = zoneName;
                            showZoneDetailsPopup(zoneName, layer);
                        }
                    });
                }
            }).addTo(map);
        })
        .catch(err => console.error("Error al cargar GeoJSON de sectores:", err));
}

// 7. DESPLEGAR POPUP DETALLADO DE RIESGO
function showZoneDetailsPopup(zoneName, layer) {
    const data = currentRiskData[zoneName];
    if (!data) {
        layer.bindPopup(`<div class="popup-title">${zoneName}</div><div>Esperando telemetría...</div>`).openPopup();
        return;
    }
    
    const isCombinedEnabled = document.getElementById('layer-combined').checked;
    let riskLevel = data.risk_level;
    let riskIndex = data.risk_index;
    
    if (!isCombinedEnabled) {
        const hasBonus = data.precipitation_mm_h >= 25 && data.tide_m >= 2.5;
        if (hasBonus) {
            riskIndex -= 10;
            if (riskIndex >= 70) riskLevel = "Crítico";
            else if (riskIndex >= 50) riskLevel = "Alto";
            else if (riskIndex >= 30) riskLevel = "Medio";
            else riskLevel = "Bajo";
        }
    }
    
    const content = `
        <div class="popup-title">${zoneName}</div>
        <span class="popup-risk-badge ${riskLevel.toLowerCase()}">${riskLevel} (Índice: ${Math.round(riskIndex)})</span>
        <div class="popup-details">
            <div class="popup-item"><span>Precipitación:</span> <strong>${data.precipitation_mm_h} mm/h</strong></div>
            <div class="popup-item"><span>Marea (INOCAR):</span> <strong>${data.tide_m} m</strong></div>
            <div class="popup-item"><span>Embalse Daule-Peripa:</span> <strong>${data.reservoir_pct}%</strong></div>
            <div class="popup-item"><span>Elevación DEM:</span> <strong>${data.elevation_m} m</strong></div>
        </div>
    `;
    
    layer.bindPopup(content).openPopup();
}

// 8. DIBUJAR MARCADORES DE ALBERGUES
function updateShelterMarkers() {
    shelterMarkersGroup.clearLayers();
    
    // Icono Personalizado para Albergues Seguros
    const shelterIcon = L.divIcon({
        html: '<div style="background-color:#0ea5e9; width:28px; height:28px; border-radius:50%; border:2px solid #ffffff; display:flex; align-items:center; justify-content:center; color:#ffffff; font-size:12px; box-shadow:0 0 10px rgba(14,165,233,0.5)"><i class="fa-solid fa-person-shelter"></i></div>',
        className: 'custom-shelter-marker',
        iconSize: [28, 28],
        iconAnchor: [14, 14]
    });

    Object.values(alberguesCoords).forEach(item => {
        L.marker([item.lat, item.lon], { icon: shelterIcon })
            .bindPopup(`<strong>Albergue Seguro: ${item.name}</strong><br>Listo para recibir evacuados de cota baja.`)
            .addTo(shelterMarkersGroup);
    });
}

// 9. FUNCIÓN DE RUTEO VIAL (OSRM API)
function calculateEvacuationRoute() {
    const selectedZoneName = document.getElementById('select-zone-evac').value;
    if (!selectedZoneName) {
        alert("Por favor, seleccione una zona de origen.");
        return;
    }
    
    const zoneData = currentRiskData[selectedZoneName];
    if (!zoneData) {
        alert("Aún no se tienen datos de coordenadas para esta zona. Intente en unos segundos.");
        return;
    }
    
    const shelter = alberguesCoords[selectedZoneName];
    if (!shelter) return;
    
    const orig_lat = zoneData.lat;
    const orig_lon = zoneData.lon;
    const dest_lat = shelter.lat;
    const dest_lon = shelter.lon;
    
    const osrmUrl = `https://router.project-osrm.org/route/v1/foot/${orig_lon},${orig_lat};${dest_lon},${dest_lat}?overview=full&geometries=geojson`;
    
    fetch(osrmUrl)
        .then(res => res.json())
        .then(data => {
            if (data.code !== "Ok" || !data.routes || data.routes.length === 0) {
                alert("No se pudo calcular la ruta por red vial.");
                return;
            }
            
            const route = data.routes[0];
            const geometry = route.geometry;
            const distance = (route.distance / 1000).toFixed(2); // km
            const time = Math.round(route.duration / 60); // minutos
            
            // Limpiar ruta previa
            if (activeRouteLine) {
                map.removeLayer(activeRouteLine);
            }
            
            // Dibujar nueva ruta en Leaflet con brillo cian
            activeRouteLine = L.geoJSON(geometry, {
                style: {
                    color: '#38bdf8',
                    weight: 5,
                    opacity: 0.85,
                    shadowColor: '#0ea5e9',
                    shadowBlur: 10
                }
            }).addTo(map);
            
            // Enfocar la ruta en el mapa
            map.fitBounds(activeRouteLine.getBounds(), { padding: [50, 50] });
            
            // Mostrar estadísticas en el Widget
            document.getElementById('route-dest').textContent = shelter.name;
            document.getElementById('route-dist').textContent = `${distance} km`;
            document.getElementById('route-time').textContent = `${time} min`;
            document.getElementById('route-result-panel').classList.remove('hidden');
        })
        .catch(err => {
            console.error("Error al calcular ruta con OSRM:", err);
            alert("Error en el servicio de mapas OSRM. Asegúrese de estar conectado a Internet.");
        });
}

function clearEvacuationRoute() {
    if (activeRouteLine) {
        map.removeLayer(activeRouteLine);
        activeRouteLine = null;
    }
    document.getElementById('route-result-panel').classList.add('hidden');
    document.getElementById('select-zone-evac').value = "";
    map.setView([-2.175, -79.910], 12.5);
}

// 10. POLLING DINÁMICO DE DATOS DESDE EL API BACKEND
function fetchWeatherData() {
    // 10.1 Riesgo actual por zona
    fetch(`${API_BASE}/riesgo/actual`)
        .then(res => res.json())
        .then(data => {
            if (data.length === 0) return;
            
            // Convertir lista a diccionario mapeado por zona
            data.forEach(item => {
                currentRiskData[item.zone] = item;
            });
            
            // Actualizar mapa y estilos de sectores
            if (sectorsLayer) {
                sectorsLayer.eachLayer(layer => {
                    sectorsLayer.resetStyle(layer);
                });
            } else {
                loadSectorsGeoJSON();
            }
            
            // Actualizar Gráfico de Precipitaciones
            const zonePrecip = [
                currentRiskData["Isla Trinitaria"]?.precipitation_mm_h || 0,
                currentRiskData["Suburbio Oeste"]?.precipitation_mm_h || 0,
                currentRiskData["Sauces"]?.precipitation_mm_h || 0,
                currentRiskData["Samanes"]?.precipitation_mm_h || 0
            ];
            precipChart.updateSeries([{ name: 'Lluvia Actual', data: zonePrecip }]);
        })
        .catch(err => console.log("Postgres/API no responde aún para riesgo actual."));

    // 10.2 Histórico SST
    fetch(`${API_BASE}/sst/historico`)
        .then(res => res.json())
        .then(data => {
            if (data.length === 0) return;
            
            // Tomar los últimos registros de temperatura y actualizar el badge del header
            const latestSST = data[0];
            const sstVal = latestSST.value;
            const state = latestSST.estado;
            
            const badge = document.getElementById('sst-status-badge');
            const dot = badge.querySelector('.status-dot');
            const txt = document.getElementById('sst-status-text');
            
            txt.textContent = `Monitoreo ENSO: SST ${sstVal.toFixed(1)}°C (${state})`;
            dot.className = "status-dot";
            if (state === "Normal") dot.classList.add("green");
            else if (state === "Fría") dot.classList.add("blue");
            else dot.classList.add("red"); // Posible El Niño
            
            // Alimentar serie de tiempo del gráfico
            // Mapear timestamps y valores
            const seriesData = data.slice(0, 20).reverse().map(item => ({
                x: new Date(item.timestamp).getTime(),
                y: item.value
            }));
            
            sstChart.updateSeries([{ name: 'Temperatura SST', data: seriesData }]);
        })
        .catch(err => console.log("Postgres/API no responde aún para historico SST."));

    // 10.3 Alertas de la SNGR
    fetch(`${API_BASE}/alertas`)
        .then(res => res.json())
        .then(data => {
            const container = document.getElementById('alerts-ticker');
            if (data.length === 0) {
                container.innerHTML = `
                    <div class="alert-item info">
                        <p class="alert-time">Monitoreo activo</p>
                        <p class="alert-text">Sin novedades críticas reportadas en las últimas horas.</p>
                    </div>
                `;
                return;
            }
            
            let html = '';
            data.forEach(alert => {
                const dateStr = new Date(alert.event_time).toLocaleTimeString('es-EC', { hour: '2-digit', minute: '2-digit' });
                html += `
                    <div class="alert-item ${alert.alert_level}">
                        <p class="alert-time">SNGR - ${dateStr} [Alerta ${alert.alert_level}]</p>
                        <p class="alert-text"><strong>${alert.zone}:</strong> ${alert.description}</p>
                    </div>
                `;
            });
            container.innerHTML = html;
        })
        .catch(err => console.log("Postgres/API no responde aún para alertas SNGR."));
}

// 11. BINDING DE COMPORTAMIENTOS E INTERRUPTORES (TOGGLES DE CAPAS)
function registerLayerEvents() {
    document.getElementById('layer-combined').addEventListener('change', function(e) {
        if (sectorsLayer) {
            sectorsLayer.eachLayer(layer => {
                sectorsLayer.resetStyle(layer);
            });
        }
    });
    
    document.getElementById('layer-rivers').addEventListener('change', function(e) {
        if (e.target.checked) riversLayer.addTo(map);
        else map.removeLayer(riversLayer);
    });
    
    document.getElementById('layer-dem').addEventListener('change', function(e) {
        if (e.target.checked) demLayer.addTo(map);
        else map.removeLayer(demLayer);
    });
    
    document.getElementById('layer-history').addEventListener('change', function(e) {
        if (e.target.checked) historyLayer.addTo(map);
        else map.removeLayer(historyLayer);
    });
    
    document.getElementById('btn-calculate-route').addEventListener('click', calculateEvacuationRoute);
    document.getElementById('btn-clear-route').addEventListener('click', clearEvacuationRoute);
}

// INICIAR TODO AL CARGAR LA PÁGINA
window.onload = function() {
    drawHydrography();
    drawDEMLayer();
    drawHistoryLayer();
    updateShelterMarkers();
    loadSectorsGeoJSON();
    registerLayerEvents();
    
    // Primer polling de telemetría y configurar loop
    fetchWeatherData();
    setInterval(fetchWeatherData, 3000); // Actualiza cada 3 segundos
};
