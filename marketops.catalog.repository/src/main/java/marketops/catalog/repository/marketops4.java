
package marketops.catalog.repository;

import io.javalin.Javalin;
import io.javalin.http.Context;
import jakarta.persistence.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import java.io.Serializable;
import java.util.*;

public class marketops4 {

    private static SessionFactory sessionFactory;

    // --- PERSISTENT MODELS ---
    
    @Entity @Table(name = "users")
    public static class User {
        @Id public String username;
        @ElementCollection(fetch = FetchType.EAGER)
        public Set<String> permissions = new HashSet<>();
        public User() {}
        public User(String username) { this.username = username.toLowerCase(); }
    }

    @Entity @Table(name = "data_sources")
    public static class DataSource { @Id public String id; public String name; public String type; public String format; public String connectionData; }

    @Entity @Table(name = "market_assets")
    public static class MarketAsset {
        @Id public String id;
        public String name;
        public String type;
        public Double demand;
        public Double currentPrice;
        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
        @JoinColumn(name = "asset_id")
        public List<MarketAssetHistory> history = new ArrayList<>();
    }

    @Entity @Table(name = "market_history")
    public static class MarketAssetHistory {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        public Long internalId;
        public Double price;
        public Double demand;
        public long timestamp = System.currentTimeMillis();
        public MarketAssetHistory() {}
        public MarketAssetHistory(Double p, Double d) { this.price = p; this.demand = d; }
    }

    @Entity @Table(name = "etl_jobs")
    public static class ETL { @Id public String id; public String name; public String triggerType; public String dataSourceId; public String dataSetId; public String path; public String language; }

    @Entity @Table(name = "lineage_tracking")
    public static class LineageTracking { @Id public String id; public String dataSourceId; public String marketAssetId; public String userId; public String modelRegistryId; public long timestamp = System.currentTimeMillis(); }

    @Entity @Table(name = "live_price_tracking")
    public static class LivePriceTracking { @Id public String id; public String name; public String modelId; public String dataSourceId; public String simulationTrackingId; public Double toleranceRangeStart; public Double toleranceRangeEnd; }

    @Entity @Table(name = "data_sets")
    public static class DataSet { @Id public String id; public String name; public String description; @Column(columnDefinition="TEXT") public String schema; public String path; }

    @Entity @Table(name = "model_registry")
    public static class ModelRegistry { 
        @Id public String id; 
        public String modelName; 
        public String description; 
        @ElementCollection(fetch = FetchType.EAGER)
        public Map<String, String> parameters = new HashMap<>(); // Simplified to String for DB storage
    }

    @Entity @Table(name = "distribution_registry")
    public static class DistributionRegistry { 
        @Id public String id; 
        public String factorName; 
        public String signalType; 
        @ElementCollection(fetch = FetchType.EAGER)
        public Map<String, Double> statistics = new HashMap<>(); 
    }

    @Entity @Table(name = "simulation_tracking")
    public static class SimulationTracking { 
        @Id public String id; 
        public String name; 
        public String equation; 
        @ElementCollection(fetch = FetchType.EAGER)
        public Map<String, String> distributionMapping = new HashMap<>(); 
    }

    // --- DB CONFIGURATION ---

    private static void initDatabase() {
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.connection.url", "jdbc:postgresql://localhost:5432/postgres");
        cfg.setProperty("hibernate.connection.username", "postgres");
        cfg.setProperty("hibernate.connection.password", "root"); // Change this
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.setProperty("hibernate.hbm2ddl.auto", "update");
        cfg.setProperty("hibernate.show_sql", "true");

        // Map all entities
        cfg.addAnnotatedClass(User.class).addAnnotatedClass(DataSource.class).addAnnotatedClass(MarketAsset.class)
           .addAnnotatedClass(MarketAssetHistory.class).addAnnotatedClass(ETL.class).addAnnotatedClass(LineageTracking.class)
           .addAnnotatedClass(LivePriceTracking.class).addAnnotatedClass(DataSet.class).addAnnotatedClass(ModelRegistry.class)
           .addAnnotatedClass(DistributionRegistry.class).addAnnotatedClass(SimulationTracking.class);

        sessionFactory = cfg.buildSessionFactory();
    }

    public static void main(String[] args) {
        initDatabase();
        bootstrapAdmin();

        Javalin app = Javalin.create().start(7000);

        // CRUD Endpoints
        setupCrud(app, "market-assets", MarketAsset.class);
        setupCrud(app, "data-sources", DataSource.class);
        setupCrud(app, "data-sets", DataSet.class);
        setupCrud(app, "etl", ETL.class);
        setupCrud(app, "lineage", LineageTracking.class);
        setupCrud(app, "models", ModelRegistry.class);
        setupCrud(app, "distributions", DistributionRegistry.class);
        setupCrud(app, "simulations", SimulationTracking.class);

        app.post("/governance/grant", ctx -> ctx.result(executeGrant(ctx.queryParam("cmd"))));

        System.out.println("\n>>> Market Ops (Postgres) Ready. Port 7000");
    }

    // --- GENERIC CRUD ---

    private static <T> void setupCrud(Javalin app, String path, Class<T> clazz) {
        String fullPath = "/catalog/" + path;

        app.post(fullPath, ctx -> {
            if (!authorize(ctx, "ADMIN")) return;
            T item = ctx.bodyAsClass(clazz);
            
            if (item instanceof MarketAsset ma) {
                ma.history.add(new MarketAssetHistory(ma.currentPrice, ma.demand));
            }

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                session.merge(item);
                tx.commit();
                ctx.status(201).json(item);
            }
        });

        app.get(fullPath, ctx -> {
            try (Session session = sessionFactory.openSession()) {
                ctx.json(session.createQuery("from " + clazz.getName(), clazz).list());
            }
        });

        app.get(fullPath + "/{id}", ctx -> {
            try (Session session = sessionFactory.openSession()) {
                T obj = session.get(clazz, (Serializable) ctx.pathParam("id"));
                if (obj != null) ctx.json(obj); else ctx.status(404);
            }
        });
    }

    // --- GOVERNANCE ENGINE ---

    private static boolean authorize(Context ctx, String action) {
        String username = ctx.header("X-User");
        if (username == null) return false;
        try (Session session = sessionFactory.openSession()) {
            User u = session.get(User.class, username.toLowerCase());
            if (u != null && (u.permissions.contains(action + ":ALL") || u.permissions.contains("ADMIN:ALL"))) return true;
        }
        ctx.status(403).result("Access Denied");
        return false;
    }

    private static String executeGrant(String command) {
        try {
            String[] parts = command.split("\\s+");
            String action = parts[1].toUpperCase();
            String target = parts[3].toUpperCase();
            String username = parts[5].toLowerCase();

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                User user = session.get(User.class, username);
                if (user == null) user = new User(username);
                user.permissions.add(action + ":" + target);
                session.merge(user);
                tx.commit();
                return "SUCCESS";
            }
        } catch (Exception e) { return "ERROR"; }
    }

    private static void bootstrapAdmin() {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            User admin = new User("admin_user");
            admin.permissions.add("ADMIN:ALL");
            session.merge(admin);
            tx.commit();
        }
    }
}