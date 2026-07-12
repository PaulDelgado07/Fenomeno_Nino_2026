// =============================================================
//  PLATAFORMA BIG DATA — MONITOREO EL NIÑO 2026
//  Dashboard interactivo · Guayaquil, Ecuador
//  Stack: Kafka → Spark → HDFS → PostgreSQL → FastAPI → Leaflet
// =============================================================

const API_BASE = "/api";

// ─── MAPA ────────────────────────────────────────────────────
const map = L.map('map', {
    zoomControl: true,
    attributionControl: false,
    preferCanvas: true
}).setView([-2.175, -79.910], 12);

// Capa base: CartoDB Positron — estándar profesional para
// dashboards de datos geoespaciales (fondo claro neutro).
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
    subdomains: 'abcd'
}).addTo(map);

// ─── ESTADO DE CAPAS ─────────────────────────────────────────
let sectorsLayer     = null;
let riversLayer      = null;
let demLayer         = null;
let historyLayer     = null;
let precipLayer      = null;
let tideLayer        = null;
let activeRouteLine  = null;
let shelterGroup     = L.layerGroup().addTo(map);
let labelsGroup      = L.layerGroup().addTo(map);

// ─── ESTADO GLOBAL ───────────────────────────────────────────
let currentRiskData  = {};     // Datos más recientes por zona
let refreshCountdown = 3;      // Contador de próximo refresh
let refreshTimer     = null;

// ─── CATÁLOGO DE ALBERGUES ───────────────────────────────────
const ALBERGUES = {
    "Isla Trinitaria": {
        lat: -2.2150, lon: -79.9000,
        name: "Coliseo Zona Alta – Trinitaria",
        capacity: 250, elevation_m: 6.5
    },
    "Suburbio Oeste": {
        lat: -2.1850, lon: -79.8970,
        name: "Coliseo Abel Jiménez Parra / U. Guayaquil",
        capacity: 500, elevation_m: 8.0
    },
    "Daule": {
        lat: -2.1054, lon: -79.8950,
        name: "Polideportivo Samanes (Norte)",
        capacity: 300, elevation_m: 9.0
    },
    "Sauces": {
        lat: -2.1054, lon: -79.8950,
        name: "Polideportivo Samanes (Norte)",
        capacity: 300, elevation_m: 9.0
    },
    "Samborondón": {
        lat: -2.1850, lon: -79.8970,
        name: "Coliseo Abel Jiménez Parra / U. Guayaquil",
        capacity: 500, elevation_m: 8.0
    },
    "Samanes": {
        lat: -2.0500, lon: -79.8800,
        name: "Colegio Fiscal Norte – Zona Alta",
        capacity: 200, elevation_m: 12.0
    }
};

// Coordenadas de origen por zona (para routing)
const ZONE_COORDS = {
    "Isla Trinitaria": { lat: -2.2370, lon: -79.9150 },
    "Suburbio Oeste":  { lat: -2.1990, lon: -79.9190 },
    "Daule":           { lat: -1.8640, lon: -79.9800 },
    "Sauces":          { lat: -2.1480, lon: -79.9000 },
    "Samborondón":     { lat: -2.0890, lon: -79.8680 },
    "Samanes":         { lat: -2.1200, lon: -79.9060 }
};

// ─── INICIALIZAR GRÁFICOS APEXCHARTS (TEMA CLARO) ────────────

// Opciones comunes para todos los gráficos
const chartDefaults = {
    chart: {
        toolbar: { show: false },
        background: 'transparent',
        fontFamily: "'Inter', sans-serif",
        animations: { enabled: true, easing: 'easeinout', speed: 600 }
    },
    theme: { mode: 'light' },
    grid: {
        borderColor: '#e8eef5',
        strokeDashArray: 3,
        xaxis: { lines: { show: false } }
    },
    tooltip: {
        style: { fontSize: '11px', fontFamily: "'Inter', sans-serif" },
        theme: 'light'
    }
};

// ── Gráfico 1: SST Niño 3.4 ───────────────────────────────
const sstChart = new ApexCharts(document.querySelector("#sst-chart"), {
    ...chartDefaults,
    chart: { ...chartDefaults.chart, type: 'bar', height: '100%', sparkline: { enabled: false } },
    plotOptions: { bar: { borderRadius: 4, columnWidth: '60%' } },
    fill: { type: 'solid' },
    colors: ['#0891b2'],
    dataLabels: { enabled: false },
    series: [{ name: 'SST (°C)', data: [] }],
    xaxis: {
        type: 'datetime',
        labels: {
            style: { fontSize: '10px' },
            datetimeUTC: false,
            format: 'HH:mm'   // ← muestra hora:minuto ya que los datos llegan cada 3s
        },
        tickAmount: 6         // ← máximo 6 etiquetas para que no se amontonen
    },
    yaxis: { title: { text: '°C', style: { fontSize: '10px' } }, min: 25, max: 32, tickAmount: 4,
             labels: { formatter: v => v.toFixed(1) + '°', style: { fontSize: '10px' } } },
    annotations: {
        yaxis: [
            { y: 28.5, borderColor: '#d97706', borderWidth: 1.5, strokeDashArray: 4,
              label: { text: 'Umbral El Niño', style: { background: '#fffbeb', color: '#92400e', fontSize: '10px', padding: { right: 6 } } } }
        ]
    },
    markers: { size: 0, hover: { size: 4 } }
});
sstChart.render();

// ── Gráfico 2: Precipitación por Zona ─────────────────────
const precipChart = new ApexCharts(document.querySelector("#precip-chart"), {
    ...chartDefaults,
    chart: { ...chartDefaults.chart, type: 'bar', height: '100%' },
    plotOptions: { bar: { borderRadius: 5, columnWidth: '58%', dataLabels: { position: 'top' } } },
    dataLabels: {
        enabled: true,
        formatter: v => v > 0 ? v.toFixed(0) : '',
        offsetY: -12,
        style: { fontSize: '10px', colors: ['#4a5e78'], fontWeight: '700' }
    },
    colors: ['#0891b2'],
    series: [{ name: 'Precipitación (mm/h)', data: [0, 0, 0, 0, 0, 0] }],
    xaxis: {
        categories: ['Trinitaria', 'Suburbio', 'Daule', 'Sauces', 'Samborondón', 'Samanes'],
        labels: { style: { fontSize: '9px' } }
    },
    yaxis: {
        title: { text: 'mm/h', style: { fontSize: '10px' } },
        max: 60,
        labels: { formatter: v => v + '', style: { fontSize: '10px' } }
    },
    annotations: {
        yaxis: [
            { y: 30, borderColor: '#ea580c', borderWidth: 1.5, strokeDashArray: 4,
              label: { text: 'Riesgo alto', style: { background: '#fff7ed', color: '#9a3412', fontSize: '10px', padding: { right: 6 } } } }
        ]
    }
});
precipChart.render();

// ── Gráfico 3: Marea + Embalse (eje dual) ─────────────────
const tideChart = new ApexCharts(document.querySelector("#tide-reservoir-chart"), {
    ...chartDefaults,
    chart: { ...chartDefaults.chart, type: 'line', height: '100%' },
    stroke: { curve: 'smooth', width: [3, 2.5], dashArray: [0, 6] },
    colors: ['#06b6d4', '#059669'],
    series: [
        { name: 'Marea INOCAR (m)', data: [] },
        { name: 'Embalse D-P (%)', data: [] }
    ],
    xaxis: { type: 'datetime', labels: { style: { fontSize: '10px' }, datetimeUTC: false } },
    yaxis: [
        { title: { text: 'Marea (m)', style: { fontSize: '10px', color: '#06b6d4' } },
          min: 0, max: 5, tickAmount: 5, labels: { formatter: v => v.toFixed(1) + 'm', style: { fontSize: '10px', colors: ['#06b6d4'] } } },
        { opposite: true,
          title: { text: 'Embalse (%)', style: { fontSize: '10px', color: '#059669' } },
          min: 0, max: 100, tickAmount: 5, labels: { formatter: v => v + '%', style: { fontSize: '10px', colors: ['#059669'] } } }
    ],
    legend: { position: 'top', fontSize: '10px', markers: { size: 8 } },
    annotations: {
        yaxis: [
            { y: 2.5, borderColor: '#06b6d4', borderWidth: 1, strokeDashArray: 3,
              label: { text: 'Umbral pleamar', style: { background: '#ecfeff', color: '#0e7490', fontSize: '10px', padding: { right: 6 } } } }
        ]
    }
});
tideChart.render();

// Almacena historial para gráfico 3
let tideHistory = [];
let reservoirHistory = [];

// ─── CAPA HIDROGRÁFICA ────────────────────────────────────────
function drawHydrography() {
    const rivers = [
        // Río Daule (desde el norte hasta la confluencia)
        { coords: [[-1.880, -79.985], [-1.950, -79.975], [-2.020, -79.960], [-2.075, -79.935],
                   [-2.115, -79.900], [-2.150, -79.878], [-2.175, -79.870]], weight: 5, label: 'Río Daule' },
        // Río Babahoyo (desde el este)
        { coords: [[-2.130, -79.785], [-2.148, -79.820], [-2.160, -79.848], [-2.172, -79.868]], weight: 4.5, label: 'Río Babahoyo' },
        // Río Guayas (unión hacia el sur)
        { coords: [[-2.175, -79.870], [-2.205, -79.883], [-2.245, -79.895], [-2.290, -79.905]], weight: 7, label: 'Río Guayas' },
        // Estero Salado (costa oeste)
        { coords: [[-2.160, -79.945], [-2.190, -79.952], [-2.215, -79.942], [-2.240, -79.930],
                   [-2.260, -79.918], [-2.280, -79.908]], weight: 3.5, label: 'Estero Salado' }
    ];

    const lines = rivers.map(r => {
        const line = L.polyline(r.coords, {
            color: '#1d64d8',
            weight: r.weight,
            opacity: 0.55,
            lineCap: 'round',
            lineJoin: 'round'
        });
        line.bindTooltip(r.label, { permanent: false, className: 'zone-label' });
        return line;
    });

    riversLayer = L.layerGroup(lines);
    if (document.getElementById('layer-rivers').checked) {
        riversLayer.addTo(map);
    }
}

// ─── CAPA TOPOGRÁFICA / DEM ───────────────────────────────────
function drawDEMLayer() {
    const zones = [
        // Zona crítica: < 2m
        { coords: [[-2.220, -79.925], [-2.240, -79.905], [-2.260, -79.920],
                   [-2.245, -79.945], [-2.222, -79.940]], fill: '#ef4444', label: 'Cota < 2m (Crítico)', opacity: 0.18 },
        // Zona baja: 2–4m
        { coords: [[-2.185, -79.935], [-2.200, -79.925], [-2.215, -79.928],
                   [-2.225, -79.940], [-2.210, -79.950], [-2.190, -79.948]], fill: '#f59e0b', label: 'Cota 2–4m (Bajo)', opacity: 0.16 },
        // Zona media: 4–8m
        { coords: [[-2.145, -79.916], [-2.160, -79.900], [-2.175, -79.905],
                   [-2.175, -79.925], [-2.155, -79.928]], fill: '#10b981', label: 'Cota 4–8m', opacity: 0.13 }
    ];

    const polygons = zones.map(z => L.polygon(z.coords, {
        fillColor: z.fill,
        fillOpacity: z.opacity,
        color: z.fill,
        weight: 1,
        dashArray: '5, 4'
    }).bindTooltip(`<strong>DEM:</strong> ${z.label}`, { sticky: true, className: 'zone-label' }));

    demLayer = L.layerGroup(polygons);
    if (document.getElementById('layer-dem').checked) demLayer.addTo(map);
}

// ─── CAPA DE PRECIPITACIÓN (burbujas dinámicas) ───────────────
function drawPrecipLayer(riskData) {
    if (precipLayer) map.removeLayer(precipLayer);

    const items = [];
    const zoneCoords = {
        "Isla Trinitaria": [-2.237, -79.908],
        "Suburbio Oeste":  [-2.199, -79.919],
        "Daule":           [-1.864, -79.980],
        "Sauces":          [-2.148, -79.900],
        "Samborondón":     [-2.089, -79.868],
        "Samanes":         [-2.120, -79.906]
    };

    Object.entries(zoneCoords).forEach(([zone, coords]) => {
        const data = riskData[zone];
        const rain = data ? data.precipitation_mm_h : 0;
        if (rain <= 0) return;

        const radius  = 800 + rain * 25;  // escalar radio con la lluvia
        const opacity = Math.min(0.12 + rain / 300, 0.38);
        const color   = rain >= 45 ? '#dc2626' : rain >= 25 ? '#ea580c' : '#0891b2';

        const circle = L.circle(coords, {
            radius, fillColor: color, fillOpacity: opacity,
            color: color, weight: 1, dashArray: '3, 5'
        }).bindTooltip(`<strong>${zone}</strong><br>Precipitación: <strong>${rain.toFixed(1)} mm/h</strong>`,
                       { sticky: true, className: 'zone-label' });
        items.push(circle);
    });

    precipLayer = L.layerGroup(items);
    if (document.getElementById('layer-precip').checked) precipLayer.addTo(map);
}

// ─── CAPA DE MAREA ────────────────────────────────────────────
function drawTideLayer(tideVal) {
    if (tideLayer) map.removeLayer(tideLayer);

    // Color y ancho de la costa según nivel de marea
    const color  = tideVal >= 3.2 ? '#dc2626' : tideVal >= 2.5 ? '#ea580c' : '#06b6d4';
    const weight = tideVal >= 3.2 ? 8 : tideVal >= 2.5 ? 5 : 3.5;
    const label  = `Marea: ${tideVal.toFixed(2)}m ${tideVal >= 3.2 ? '⚠️ Pleamar máxima' : tideVal >= 2.5 ? '⚡ Pleamar alta' : ''}`;

    const coastline = [
        [-2.155, -79.958], [-2.175, -79.962], [-2.200, -79.958],
        [-2.225, -79.955], [-2.248, -79.945], [-2.270, -79.925],
        [-2.285, -79.910]
    ];

    const line = L.polyline(coastline, {
        color, weight, opacity: 0.7, lineCap: 'round', lineJoin: 'round'
    }).bindTooltip(label, { sticky: true, className: 'zone-label' });

    tideLayer = L.layerGroup([line]);
    if (document.getElementById('layer-tide').checked) tideLayer.addTo(map);
}

// ─── ZONAS HISTÓRICAMENTE INUNDABLES ─────────────────────────
function drawHistoryLayer() {
    const spots = [
        { name: "Sauces 6 — Inundación recurrente, influencia Río Daule", coords: [-2.143, -79.897] },
        { name: "Sauces 3 — Colapso de alcantarillado", coords: [-2.151, -79.893] },
        { name: "Av. Barcelona — Desbordamiento Estero Salado", coords: [-2.194, -79.924] },
        { name: "Trinitaria Sur — Anegación en pleamar máxima", coords: [-2.242, -79.906] },
        { name: "Suburbio Oeste – Av. 25 de Julio — Inundaciones crónicas", coords: [-2.202, -79.916] },
        { name: "Daule Centro — Crecida Río Daule 1997/1998", coords: [-1.866, -79.978] }
    ];

    const markers = spots.map(s => L.circleMarker(s.coords, {
        radius: 9,
        fillColor: '#a855f7',
        fillOpacity: 0.7,
        color: '#ffffff',
        weight: 2
    }).bindPopup(`
        <div style="padding:0.4rem">
            <strong style="font-size:0.8rem">📍 Zona Histórica de Riesgo</strong><br>
            <span style="font-size:0.7rem;color:#4a5e78">${s.name}</span>
        </div>
    `));

    historyLayer = L.layerGroup(markers);
    if (document.getElementById('layer-history').checked) historyLayer.addTo(map);
}

// ─── ESTILO DEL POLÍGONO POR NIVEL DE RIESGO ─────────────────
function getRiskStyle(feature) {
    const zoneName = feature.properties.name;
    const data     = currentRiskData[zoneName];

    if (!data) {
        return { fillColor: '#94a3b8', fillOpacity: 0.12, color: '#64748b', weight: 1.5, dashArray: '4,3' };
    }

    const isCombined = document.getElementById('layer-combined').checked;
    let riskLevel    = data.risk_level || 'Bajo';
    let riskIndex    = data.risk_index || 0;

    // Si el escenario combinado está desactivado, restar el bonus
    if (!isCombined) {
        const hasBonus = data.precipitation_mm_h >= 25 && data.tide_m >= 2.5;
        if (hasBonus) {
            riskIndex = Math.max(0, riskIndex - 10);
            riskLevel = riskIndex >= 70 ? 'Crítico' : riskIndex >= 50 ? 'Alto' : riskIndex >= 30 ? 'Medio' : 'Bajo';
        }
    }

    const palette = {
        'Bajo':    { fill: '#059669', border: '#047857' },
        'Medio':   { fill: '#d97706', border: '#b45309' },
        'Alto':    { fill: '#ea580c', border: '#c2410c' },
        'Crítico': { fill: '#dc2626', border: '#b91c1c' }
    };

    const p = palette[riskLevel] || palette['Bajo'];
    const isCrit = riskLevel === 'Crítico';

    return {
        fillColor:   p.fill,
        fillOpacity: isCrit ? 0.55 : 0.35,
        color:       p.border,
        weight:      isCrit ? 2.5 : 1.5,
        dashArray:   isCrit ? '' : '4,2'
    };
}

// ─── CARGAR SECTORES GEOJSON ──────────────────────────────────
function loadSectorsGeoJSON() {
    fetch('data/gye_sectors.geojson')
        .then(r => r.json())
        .then(geojson => {
            if (sectorsLayer) map.removeLayer(sectorsLayer);
            labelsGroup.clearLayers();

            sectorsLayer = L.geoJSON(geojson, {
                style: getRiskStyle,
                onEachFeature: (feature, layer) => {
                    layer.on({
                        mouseover: e => {
                            e.target.setStyle({ fillOpacity: 0.70, weight: 2.5 });
                        },
                        mouseout: e => {
                            sectorsLayer.resetStyle(e.target);
                        },
                        click: e => {
                            const name = feature.properties.name;
                            document.getElementById('select-zone-evac').value = name;
                            showZonePopup(name, layer);
                            map.fitBounds(e.target.getBounds(), { padding: [60, 60] });
                        }
                    });

                    // Etiqueta permanente con nombre de zona
                    const center = layer.getBounds().getCenter();
                    const data   = currentRiskData[feature.properties.name];
                    const riskTxt = data ? ` · ${data.risk_level || ''}` : '';

                    L.marker(center, {
                        icon: L.divIcon({
                            className: '',
                            html: `<div class="zone-label">${feature.properties.name}${riskTxt}</div>`,
                            iconAnchor: [50, 10],
                            iconSize: [100, 20]
                        }),
                        interactive: false,
                        zIndexOffset: 100
                    }).addTo(labelsGroup);
                }
            }).addTo(map);
        })
        .catch(err => console.warn("GeoJSON no disponible:", err));
}

// Actualizar colores + etiquetas de sectores cuando llegan datos nuevos
function refreshSectorsStyle() {
    if (!sectorsLayer) return;
    labelsGroup.clearLayers();
    sectorsLayer.eachLayer(layer => {
        const name  = layer.feature.properties.name;
        const data  = currentRiskData[name];
        const rl    = data ? (data.risk_level || '') : '';
        sectorsLayer.resetStyle(layer);

        const center = layer.getBounds().getCenter();
        L.marker(center, {
            icon: L.divIcon({
                className: '',
                html: `<div class="zone-label">${name}${rl ? ' · ' + rl : ''}</div>`,
                iconAnchor: [50, 10],
                iconSize: [100, 20]
            }),
            interactive: false,
            zIndexOffset: 100
        }).addTo(labelsGroup);
    });
}

// ─── POPUP FICHA TÉCNICA ──────────────────────────────────────
function showZonePopup(zoneName, layer) {
    const d = currentRiskData[zoneName];

    if (!d) {
        layer.bindPopup(`
            <div class="popup-header">
                <div class="popup-title">${zoneName}</div>
                <span class="popup-risk-badge bajo">Sin datos aún</span>
            </div>
            <div class="popup-details">
                <p style="font-size:0.68rem;color:#4a5e78">Esperando telemetría del pipeline...</p>
            </div>
        `).openPopup();
        return;
    }

    const isCombined = document.getElementById('layer-combined').checked;
    let riskLevel = d.risk_level || 'Bajo';
    let riskIndex = d.risk_index || 0;

    if (!isCombined) {
        const hasBonus = d.precipitation_mm_h >= 25 && d.tide_m >= 2.5;
        if (hasBonus) {
            riskIndex = Math.max(0, riskIndex - 10);
            riskLevel = riskIndex >= 70 ? 'Crítico' : riskIndex >= 50 ? 'Alto' : riskIndex >= 30 ? 'Medio' : 'Bajo';
        }
    }

    const badgeClass = riskLevel.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    const riskEmoji  = { 'Crítico': '🔴', 'Alto': '🟠', 'Medio': '🟡', 'Bajo': '🟢' }[riskLevel] || '⚪';

    const combinedBonus = d.precipitation_mm_h >= 25 && d.tide_m >= 2.5 ? 10 : 0;
    const vuln = Math.round((d.base_vulnerability || 0) * 20);

    layer.bindPopup(`
        <div class="popup-header">
            <div class="popup-title">${riskEmoji} ${zoneName}</div>
            <span class="popup-risk-badge ${badgeClass}">${riskLevel} — Índice: ${Math.round(riskIndex)}/100</span>
        </div>
        <div class="popup-details">
            <div class="popup-item">
                <span>🌧️ Precipitación (GPM):</span>
                <strong>${(d.precipitation_mm_h || 0).toFixed(1)} mm/h</strong>
            </div>
            <div class="popup-item">
                <span>🌊 Marea (INOCAR):</span>
                <strong>${(d.tide_m || 0).toFixed(2)} m</strong>
            </div>
            <div class="popup-item">
                <span>💧 Embalse Daule-Peripa:</span>
                <strong>${(d.reservoir_pct || 0).toFixed(1)}%</strong>
            </div>
            <div class="popup-item">
                <span>⛰️ Elevación DEM:</span>
                <strong>${(d.elevation_m || 0).toFixed(1)} m s.n.m.</strong>
            </div>
            <div class="popup-item">
                <span>⚡ Vulnerabilidad base:</span>
                <strong>${Math.round((d.base_vulnerability || 0) * 100)}%</strong>
            </div>
            ${combinedBonus > 0 && isCombined ? `
            <div class="popup-item" style="color:#9a3412">
                <span>🔀 Bonus escenario combinado:</span>
                <strong>+${combinedBonus}</strong>
            </div>` : ''}
            <div class="popup-formula">
                <strong>Fórmula Spark (Structured Streaming):</strong><br>
                Riesgo = Precip(${d.precipitation_mm_h >= 45 ? 35 : d.precipitation_mm_h >= 25 ? 22 : 8}) + Marea(${d.tide_m >= 3.2 ? 25 : d.tide_m >= 2.5 ? 15 : 5}) + Embalse(${d.reservoir_pct >= 90 ? 15 : d.reservoir_pct >= 80 ? 8 : 3}) + Vulnerab.(${vuln})${combinedBonus > 0 && isCombined ? ' + Combinado(+10)' : ''} = <strong>${Math.round(riskIndex)}</strong>
            </div>
        </div>
    `).openPopup();
}

// ─── MARCADORES DE ALBERGUES ──────────────────────────────────
function drawShelters() {
    shelterGroup.clearLayers();

    const icon = L.divIcon({
        html: `<div style="
            background: linear-gradient(135deg, #0556a0, #1a8de0);
            width: 30px; height: 30px;
            border-radius: 50% 50% 50% 0;
            transform: rotate(-45deg);
            border: 2.5px solid #fff;
            display: flex; align-items: center; justify-content: center;
            box-shadow: 0 3px 12px rgba(5,86,160,0.4);">
            <i class="fa-solid fa-person-shelter" style="transform:rotate(45deg);color:#fff;font-size:11px"></i>
        </div>`,
        className: 'custom-shelter-marker',
        iconSize: [30, 30],
        iconAnchor: [15, 30]
    });

    // Evitar duplicados: renderizar albergues únicos
    const rendered = new Set();
    Object.values(ALBERGUES).forEach(a => {
        const key = `${a.lat},${a.lon}`;
        if (rendered.has(key)) return;
        rendered.add(key);

        L.marker([a.lat, a.lon], { icon })
            .bindPopup(`
                <div style="padding:0.4rem">
                    <strong style="font-size:0.82rem">🏠 ${a.name}</strong><br>
                    <span style="font-size:0.7rem;color:#4a5e78">
                        Capacidad: <strong>${a.capacity} personas</strong><br>
                        Cota segura: <strong>${a.elevation_m} m s.n.m.</strong>
                    </span>
                </div>
            `)
            .addTo(shelterGroup);
    });
}

// ─── RUTAS DE EVACUACIÓN (OSRM) ───────────────────────────────
function calculateEvacuationRoute() {
    const zone = document.getElementById('select-zone-evac').value;
    if (!zone) { alert("Por favor, seleccione una zona de origen."); return; }

    const shelter  = ALBERGUES[zone];
    const origCoord = ZONE_COORDS[zone] || currentRiskData[zone];

    if (!shelter || !origCoord) {
        alert("No hay datos de coordenadas para esta zona todavía.");
        return;
    }

    const btn = document.getElementById('btn-calculate-route');
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Calculando...';
    btn.disabled = true;

    // Limpiar ruta previa
    if (activeRouteLine) { map.removeLayer(activeRouteLine); activeRouteLine = null; }

    const origLon = origCoord.lon || origCoord.lon;
    const origLat = origCoord.lat;
    const destLon = shelter.lon;
    const destLat = shelter.lat;

    const osrmUrl = `https://router.project-osrm.org/route/v1/foot/${origLon},${origLat};${destLon},${destLat}?overview=full&geometries=geojson&steps=false`;

    fetch(osrmUrl)
        .then(r => r.json())
        .then(data => {
            btn.innerHTML = '<i class="fa-solid fa-location-arrow"></i> Calcular Ruta de Evacuación';
            btn.disabled = false;

            if (data.code !== "Ok" || !data.routes?.length) {
                alert("No se pudo calcular la ruta. Verifica la conexión a Internet.");
                return;
            }

            const route    = data.routes[0];
            const distKm   = (route.distance / 1000).toFixed(2);
            const timeMin  = Math.round(route.duration / 60);

            // Pintar la ruta en el mapa
            activeRouteLine = L.geoJSON(route.geometry, {
                style: {
                    color: '#22d3ee',
                    weight: 5.5,
                    opacity: 0.90,
                    lineCap: 'round',
                    lineJoin: 'round'
                }
            }).addTo(map);

            // Marcadores de origen y destino
            L.circleMarker([origLat, origLon], {
                radius: 10, fillColor: '#dc2626', fillOpacity: 0.9,
                color: '#fff', weight: 2
            }).bindTooltip('📍 Origen: ' + zone, { permanent: false }).addTo(activeRouteLine);

            L.circleMarker([destLat, destLon], {
                radius: 10, fillColor: '#059669', fillOpacity: 0.9,
                color: '#fff', weight: 2
            }).bindTooltip('🏠 Albergue: ' + shelter.name, { permanent: false }).addTo(activeRouteLine);

            map.fitBounds(activeRouteLine.getBounds(), { padding: [60, 60] });

            // Mostrar estadísticas
            document.getElementById('route-dest').textContent = shelter.name;
            document.getElementById('route-dist').textContent = `${distKm} km`;
            document.getElementById('route-time').textContent = `${timeMin} min (~${Math.round(timeMin / 60 * 10) / 10} h)`;
            document.getElementById('route-result-panel').classList.remove('hidden');
        })
        .catch(err => {
            btn.innerHTML = '<i class="fa-solid fa-location-arrow"></i> Calcular Ruta de Evacuación';
            btn.disabled = false;
            console.error("OSRM error:", err);
            alert("Error al contactar el servicio de ruteo OSRM. Asegúrese de tener conexión a Internet.");
        });
}

function clearEvacuationRoute() {
    if (activeRouteLine) { map.removeLayer(activeRouteLine); activeRouteLine = null; }
    document.getElementById('route-result-panel').classList.add('hidden');
    document.getElementById('select-zone-evac').value = "";
    map.setView([-2.175, -79.910], 12);
}

// ─── POLLING DE DATOS ─────────────────────────────────────────
function fetchAllData() {
    // Spinner de refresh
    const icon = document.getElementById('refresh-icon');
    if (icon) icon.classList.add('spinning');

    Promise.all([
        fetchRiskData(),
        fetchSSTData(),
        fetchAlerts()
    ]).finally(() => {
        if (icon) icon.classList.remove('spinning');
        startRefreshCountdown();
    });
}

function fetchRiskData() {
    return fetch(`${API_BASE}/riesgo/actual`)
        .then(r => r.json())
        .then(data => {
            if (!data.length) return;

            // Guardar en caché
            data.forEach(item => { currentRiskData[item.zone] = item; });

            // Actualizar mapa
            if (sectorsLayer) refreshSectorsStyle();
            else loadSectorsGeoJSON();

            // Actualizar capas dinámicas
            const sample = data[0];
            drawPrecipLayer(currentRiskData);
            if (sample) drawTideLayer(sample.tide_m || 2.0);

            // Actualizar KPI
            const maxRain = Math.max(...data.map(d => d.precipitation_mm_h || 0));
            const tideVal = data[0]?.tide_m || 0;
            const resVal  = data[0]?.reservoir_pct || 0;
            const alertZones = data.filter(d => (d.risk_level === 'Alto' || d.risk_level === 'Crítico')).length;

            updateKPI('kpi-rain-val', `${maxRain.toFixed(1)} mm/h`,
                maxRain >= 45 ? 'critical' : maxRain >= 25 ? 'danger' : maxRain >= 10 ? 'warning' : 'normal');
            updateKPI('kpi-tide-val', `${tideVal.toFixed(2)} m`,
                tideVal >= 3.2 ? 'critical' : tideVal >= 2.5 ? 'danger' : 'normal');
            updateKPI('kpi-reservoir-val', `${resVal.toFixed(1)}%`,
                resVal >= 90 ? 'critical' : resVal >= 80 ? 'danger' : 'normal');
            updateKPI('kpi-alerts-val', `${alertZones} zona${alertZones !== 1 ? 's' : ''}`,
                alertZones >= 3 ? 'critical' : alertZones >= 1 ? 'danger' : 'normal');

            // Actualizar gráfico de precipitación
            const zones = ['Isla Trinitaria', 'Suburbio Oeste', 'Daule', 'Sauces', 'Samborondón', 'Samanes'];
            const precipVals = zones.map(z => +(currentRiskData[z]?.precipitation_mm_h || 0).toFixed(1));
            precipChart.updateSeries([{ name: 'Precipitación (mm/h)', data: precipVals }]);

            // Actualizar gráfico de marea + embalse
            const now = Date.now();
            tideHistory.push({ x: now, y: +tideVal.toFixed(2) });
            reservoirHistory.push({ x: now, y: +resVal.toFixed(1) });
            if (tideHistory.length > 30) tideHistory.shift();
            if (reservoirHistory.length > 30) reservoirHistory.shift();
            tideChart.updateSeries([
                { name: 'Marea INOCAR (m)', data: [...tideHistory] },
                { name: 'Embalse D-P (%)', data: [...reservoirHistory] }
            ]);
        })
        .catch(() => {}); // silencioso si Postgres aún no responde
}

function fetchSSTData() {
    return fetch(`${API_BASE}/sst/historico`)
        .then(r => r.json())
        .then(data => {
            if (!data.length) return;

            const latest = data[0];
            const sst    = latest.value;
            const estado = latest.estado;

            // Actualizar KPI SST
            updateKPI('kpi-sst-val', `${sst.toFixed(2)}°C`,
                sst >= 29.5 ? 'critical' : sst >= 28.5 ? 'danger' : sst >= 27.5 ? 'warning' : 'normal');

            // Calcular anomalía (umbral climatológico aproximado: 27.0°C)
            const anomaly = (sst - 27.0).toFixed(2);
            const anomSign = anomaly > 0 ? '+' : '';
            updateKPI('kpi-anom-val', `${anomSign}${anomaly}°C (${estado})`,
                sst >= 28.5 ? 'critical' : sst >= 27.5 ? 'danger' : 'normal');

            // Actualizar badge ENSO
            const badge = document.getElementById('sst-status-badge');
            const dot   = badge.querySelector('.status-dot');
            document.getElementById('sst-status-text').textContent = `ENSO: ${sst.toFixed(1)}°C — ${estado}`;
            dot.className = 'status-dot';
            if (estado === 'Normal') dot.classList.add('green');
            else if (estado === 'Fría') dot.classList.add('blue');
            else if (sst >= 29.5) dot.classList.add('red');
            else dot.classList.add('yellow');

            // Actualizar gráfico SST
            const series = data.slice(0, 30).reverse().map(d => ({
                x: new Date(d.timestamp).getTime(),
                y: +d.value.toFixed(2)
            }));
            sstChart.updateSeries([{ name: 'SST (°C)', data: series }]);
        })
        .catch(() => {});
}

function fetchAlerts() {
    return fetch(`${API_BASE}/alertas`)
        .then(r => r.json())
        .then(data => {
            const container = document.getElementById('alerts-ticker');

            if (!data.length) {
                container.innerHTML = `
                    <div class="alert-item info">
                        <p class="alert-time">Pipeline activo · Sin novedades</p>
                        <p class="alert-text">No se han reportado alertas críticas en las últimas horas.</p>
                    </div>`;
                return;
            }

            container.innerHTML = data.map(a => {
                const time = new Date(a.event_time).toLocaleTimeString('es-EC', { hour: '2-digit', minute: '2-digit' });
                const lvlIcon = { 'Roja': '🔴', 'Naranja': '🟠', 'Amarilla': '🟡', 'Verde': '🟢' }[a.alert_level] || '⚪';
                return `
                    <div class="alert-item ${a.alert_level}">
                        <p class="alert-time">SNGR · ${time} · Alerta ${a.alert_level} ${lvlIcon}</p>
                        <p class="alert-text"><strong>${a.zone}:</strong> ${a.description}</p>
                    </div>`;
            }).join('');
        })
        .catch(() => {});
}

// ─── HELPER: Actualizar KPI ───────────────────────────────────
function updateKPI(elId, text, level) {
    const el = document.getElementById(elId);
    if (!el) return;
    el.textContent = text;
    el.className = 'kpi-value ' + (level || '');
}

// ─── CONTADOR DE REFRESH ──────────────────────────────────────
function startRefreshCountdown() {
    clearInterval(refreshTimer);
    refreshCountdown = 3;
    const countEl = document.getElementById('refresh-countdown');
    refreshTimer = setInterval(() => {
        refreshCountdown--;
        if (countEl) countEl.textContent = refreshCountdown + 's';
        if (refreshCountdown <= 0) {
            clearInterval(refreshTimer);
            if (countEl) countEl.textContent = '...';
        }
    }, 1000);
}

// ─── BINDINGS DE SWITCHES ─────────────────────────────────────
function registerLayerEvents() {
    document.getElementById('layer-combined').addEventListener('change', () => {
        refreshSectorsStyle();
    });

    document.getElementById('layer-rivers').addEventListener('change', e => {
        if (!riversLayer) return;
        e.target.checked ? riversLayer.addTo(map) : map.removeLayer(riversLayer);
    });

    document.getElementById('layer-dem').addEventListener('change', e => {
        if (!demLayer) return;
        e.target.checked ? demLayer.addTo(map) : map.removeLayer(demLayer);
    });

    document.getElementById('layer-precip').addEventListener('change', e => {
        if (!precipLayer) return;
        e.target.checked ? precipLayer.addTo(map) : map.removeLayer(precipLayer);
    });

    document.getElementById('layer-tide').addEventListener('change', e => {
        if (!tideLayer) return;
        e.target.checked ? tideLayer.addTo(map) : map.removeLayer(tideLayer);
    });

    document.getElementById('layer-history').addEventListener('change', e => {
        if (!historyLayer) return;
        e.target.checked ? historyLayer.addTo(map) : map.removeLayer(historyLayer);
    });

    document.getElementById('btn-calculate-route').addEventListener('click', calculateEvacuationRoute);
    document.getElementById('btn-clear-route').addEventListener('click', clearEvacuationRoute);
}

// ─── INICIALIZACIÓN ───────────────────────────────────────────
window.onload = function () {
    drawHydrography();
    drawDEMLayer();
    drawHistoryLayer();
    drawShelters();
    loadSectorsGeoJSON();
    registerLayerEvents();

    // Primer poll + loop cada 3 segundos
    fetchAllData();
    setInterval(fetchAllData, 3000);
};
