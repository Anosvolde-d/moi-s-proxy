/**
 * Visual Effects System
 * 1. Animated brilliant black lines moving across the screen
 * 2. Red and blue particle flock following the cursor
 */

(function() {
    'use strict';

    // ==========================================
    // ANIMATED LINES SYSTEM - Brilliant Black Lines
    // ==========================================

    class AnimatedLines {
        constructor() {
            this.canvas = document.getElementById('linesCanvas');
            if (!this.canvas) {
                console.error('linesCanvas not found!');
                return;
            }
            
            this.ctx = this.canvas.getContext('2d');
            this.lines = [];
            this.running = true;
            
            this.resize();
            window.addEventListener('resize', () => this.resize());
            this.createLines();
            this.animate();
            
            console.log('AnimatedLines initialized with', this.lines.length, 'lines');
        }
        
        resize() {
            this.canvas.width = window.innerWidth;
            this.canvas.height = window.innerHeight;
            console.log('Lines canvas resized to', this.canvas.width, 'x', this.canvas.height);
        }
        
        createLines() {
            this.lines = [];
            const w = this.canvas.width;
            const h = this.canvas.height;
            
            // Horizontal lines (left to right)
            for (let i = 0; i < 8; i++) {
                this.lines.push({
                    type: 'horizontal',
                    direction: 1,
                    y: Math.random() * h,
                    x: -300 - Math.random() * 500,
                    speed: 1.5 + Math.random() * 2.5,
                    length: 150 + Math.random() * 250,
                    thickness: 2 + Math.random() * 3
                });
            }
            
            // Horizontal lines (right to left)
            for (let i = 0; i < 6; i++) {
                this.lines.push({
                    type: 'horizontal',
                    direction: -1,
                    y: Math.random() * h,
                    x: w + 300 + Math.random() * 500,
                    speed: 1.5 + Math.random() * 2.5,
                    length: 150 + Math.random() * 250,
                    thickness: 2 + Math.random() * 3
                });
            }
            
            // Vertical lines (top to bottom)
            for (let i = 0; i < 6; i++) {
                this.lines.push({
                    type: 'vertical',
                    direction: 1,
                    x: Math.random() * w,
                    y: -300 - Math.random() * 500,
                    speed: 1.5 + Math.random() * 2.5,
                    length: 150 + Math.random() * 250,
                    thickness: 2 + Math.random() * 3
                });
            }
            
            // Vertical lines (bottom to top)
            for (let i = 0; i < 4; i++) {
                this.lines.push({
                    type: 'vertical',
                    direction: -1,
                    x: Math.random() * w,
                    y: h + 300 + Math.random() * 500,
                    speed: 1.5 + Math.random() * 2.5,
                    length: 150 + Math.random() * 250,
                    thickness: 2 + Math.random() * 3
                });
            }
        }
        
        drawLine(line) {
            const ctx = this.ctx;
            ctx.save();
            
            let startX, startY, endX, endY;
            
            if (line.type === 'horizontal') {
                startX = line.x;
                startY = line.y;
                endX = line.x + line.length * line.direction;
                endY = line.y;
            } else {
                startX = line.x;
                startY = line.y;
                endX = line.x;
                endY = line.y + line.length * line.direction;
            }
            
            // Create gradient for brilliant effect
            const gradient = ctx.createLinearGradient(startX, startY, endX, endY);
            gradient.addColorStop(0, 'rgba(0, 0, 0, 0)');
            gradient.addColorStop(0.2, 'rgba(30, 30, 30, 0.6)');
            gradient.addColorStop(0.5, 'rgba(100, 100, 100, 1)'); // Bright center
            gradient.addColorStop(0.8, 'rgba(30, 30, 30, 0.6)');
            gradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
            
            // Draw soft glow
            ctx.strokeStyle = 'rgba(50, 50, 50, 0.4)';
            ctx.lineWidth = line.thickness + 8;
            ctx.lineCap = 'round';
            ctx.beginPath();
            ctx.moveTo(startX, startY);
            ctx.lineTo(endX, endY);
            ctx.stroke();
            
            // Draw main line with gradient
            ctx.strokeStyle = gradient;
            ctx.lineWidth = line.thickness;
            ctx.lineCap = 'round';
            ctx.beginPath();
            ctx.moveTo(startX, startY);
            ctx.lineTo(endX, endY);
            ctx.stroke();
            
            // Draw bright core
            ctx.strokeStyle = 'rgba(150, 150, 150, 0.8)';
            ctx.lineWidth = line.thickness * 0.3;
            ctx.beginPath();
            ctx.moveTo(startX, startY);
            ctx.lineTo(endX, endY);
            ctx.stroke();
            
            ctx.restore();
        }
        
        updateLine(line) {
            const w = this.canvas.width;
            const h = this.canvas.height;
            
            if (line.type === 'horizontal') {
                line.x += line.speed * line.direction;
                
                if (line.direction === 1 && line.x > w + 100) {
                    line.x = -line.length - 100;
                    line.y = Math.random() * h;
                } else if (line.direction === -1 && line.x < -line.length - 100) {
                    line.x = w + 100;
                    line.y = Math.random() * h;
                }
            } else {
                line.y += line.speed * line.direction;
                
                if (line.direction === 1 && line.y > h + 100) {
                    line.y = -line.length - 100;
                    line.x = Math.random() * w;
                } else if (line.direction === -1 && line.y < -line.length - 100) {
                    line.y = h + 100;
                    line.x = Math.random() * w;
                }
            }
        }
        
        animate() {
            if (!this.running) return;
            
            this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
            
            for (const line of this.lines) {
                this.updateLine(line);
                this.drawLine(line);
            }
            
            requestAnimationFrame(() => this.animate());
        }
    }

    // ==========================================
    // PARTICLE FLOCK SYSTEM (Cursor Following)
    // ==========================================

    class ParticleFlock {
        constructor() {
            this.canvas = document.getElementById('particleCanvas');
            if (!this.canvas) {
                console.error('particleCanvas not found!');
                return;
            }
            
            this.ctx = this.canvas.getContext('2d');
            this.particles = [];
            this.mouse = { 
                x: window.innerWidth / 2, 
                y: window.innerHeight / 2, 
                vx: 0, 
                vy: 0 
            };
            this.lastMouse = { 
                x: window.innerWidth / 2, 
                y: window.innerHeight / 2 
            };
            this.particleCount = 60;
            this.running = true;
            
            this.resize();
            window.addEventListener('resize', () => this.resize());
            this.createParticles();
            this.setupEventListeners();
            this.animate();
            
            console.log('ParticleFlock initialized with', this.particles.length, 'particles');
        }
        
        resize() {
            this.canvas.width = window.innerWidth;
            this.canvas.height = window.innerHeight;
            console.log('Particle canvas resized to', this.canvas.width, 'x', this.canvas.height);
        }
        
        createParticles() {
            this.particles = [];
            
            for (let i = 0; i < this.particleCount; i++) {
                const isRed = i % 2 === 0;
                
                this.particles.push({
                    x: this.mouse.x + (Math.random() - 0.5) * 200,
                    y: this.mouse.y + (Math.random() - 0.5) * 200,
                    vx: 0,
                    vy: 0,
                    size: 5 + Math.random() * 7,
                    color: isRed ? '#ff3366' : '#3366ff',
                    glowColor: isRed ? 'rgba(255, 51, 102, 0.4)' : 'rgba(51, 102, 255, 0.4)',
                    isRed: isRed,
                    friction: 0.92,
                    attraction: 0.012 + Math.random() * 0.008,
                    offsetAngle: Math.random() * Math.PI * 2,
                    offsetSpeed: 0.01 + Math.random() * 0.015,
                    orbitRadius: 30 + Math.random() * 70
                });
            }
        }
        
        setupEventListeners() {
            document.addEventListener('mousemove', (e) => {
                this.mouse.vx = e.clientX - this.lastMouse.x;
                this.mouse.vy = e.clientY - this.lastMouse.y;
                this.lastMouse.x = this.mouse.x;
                this.lastMouse.y = this.mouse.y;
                this.mouse.x = e.clientX;
                this.mouse.y = e.clientY;
            });
            
            document.addEventListener('touchmove', (e) => {
                if (e.touches.length > 0) {
                    const touch = e.touches[0];
                    this.mouse.vx = touch.clientX - this.lastMouse.x;
                    this.mouse.vy = touch.clientY - this.lastMouse.y;
                    this.lastMouse.x = this.mouse.x;
                    this.lastMouse.y = this.mouse.y;
                    this.mouse.x = touch.clientX;
                    this.mouse.y = touch.clientY;
                }
            }, { passive: true });
        }
        
        updateParticle(p) {
            p.offsetAngle += p.offsetSpeed;
            
            const targetX = this.mouse.x + Math.cos(p.offsetAngle) * p.orbitRadius;
            const targetY = this.mouse.y + Math.sin(p.offsetAngle) * p.orbitRadius;
            
            const dx = targetX - p.x;
            const dy = targetY - p.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            
            if (dist > 1) {
                p.vx += (dx / dist) * p.attraction * Math.min(dist, 120);
                p.vy += (dy / dist) * p.attraction * Math.min(dist, 120);
            }
            
            p.vx += this.mouse.vx * 0.025;
            p.vy += this.mouse.vy * 0.025;
            
            p.vx *= p.friction;
            p.vy *= p.friction;
            
            p.x += p.vx;
            p.y += p.vy;
        }
        
        drawParticle(p) {
            const ctx = this.ctx;
            
            // Outer glow
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size + 10, 0, Math.PI * 2);
            ctx.fillStyle = p.glowColor;
            ctx.fill();
            
            // Inner glow
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size + 4, 0, Math.PI * 2);
            ctx.fillStyle = p.isRed 
                ? 'rgba(255, 51, 102, 0.5)' 
                : 'rgba(51, 102, 255, 0.5)';
            ctx.fill();
            
            // Main dot
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
            ctx.fillStyle = p.color;
            ctx.fill();
            
            // Bright center
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size * 0.35, 0, Math.PI * 2);
            ctx.fillStyle = p.isRed 
                ? 'rgba(255, 180, 200, 0.9)' 
                : 'rgba(180, 200, 255, 0.9)';
            ctx.fill();
        }
        
        animate() {
            if (!this.running) return;
            
            this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
            
            for (const p of this.particles) {
                this.updateParticle(p);
                this.drawParticle(p);
            }
            
            this.mouse.vx *= 0.9;
            this.mouse.vy *= 0.9;
            
            requestAnimationFrame(() => this.animate());
        }
    }

    // ==========================================
    // INITIALIZE ON DOM READY
    // ==========================================

    function init() {
        console.log('=== Initializing Visual Effects ===');
        
        const linesCanvas = document.getElementById('linesCanvas');
        const particleCanvas = document.getElementById('particleCanvas');
        
        console.log('linesCanvas found:', !!linesCanvas);
        console.log('particleCanvas found:', !!particleCanvas);
        
        if (linesCanvas) {
            window.animatedLines = new AnimatedLines();
        }
        
        if (particleCanvas) {
            window.particleFlock = new ParticleFlock();
        }
        
        console.log('=== Visual Effects Initialized ===');
    }

    // Run when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
