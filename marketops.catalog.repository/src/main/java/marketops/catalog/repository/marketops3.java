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
import java.util.stream.Collectors;

public class marketops3 {

    private static SessionFactory sessionFactory;

    // --- ENTITIES (Models) ---

    @Entity @Table(name = "users")
    public static class User {
        @Id public String username;
        @ElementCollection(fetch = FetchType.EAGER)
        public Set<String> permissions = new HashSet<>();
        public User() {}
        public User(String username) { this.username = username.toLowerCase(); }
    }

    @Entity @Table(name = "market_assets")
    public static class MarketAsset {
        @Id public String id;
        public String name;
        public Double currentPrice;
        public Double demand;
        
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

    @Entity @Table(name = "data_sources")
    public static class DataSource { @Id public String id; public String name; public String type; }

    // --- DATABASE CONFIGURATION ---

    private static void initDatabase() {
        Configuration cfg = new Configuration();
        
        // Database connection settings
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.connection.url", "jdbc:postgresql://localhost:5432/postgres");
        cfg.setProperty("hibernate.connection.username", "postgres");
        cfg.setProperty("hibernate.connection.password", "root");
        
        // Hibernate settings
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.setProperty("hibernate.hbm2ddl.auto", "update"); // Automatically creates tables
        cfg.setProperty("hibernate.show_sql", "true");

        // Register Entities
        cfg.addAnnotatedClass(User.class);
        cfg.addAnnotatedClass(MarketAsset.class);
        cfg.addAnnotatedClass(MarketAssetHistory.class);
        cfg.addAnnotatedClass(DataSource.class);

        sessionFactory = cfg.buildSessionFactory();
    }

    // --- MAIN APP ---

    public static void main(String[] args) {
        initDatabase();
        bootstrapAdmin();

        Javalin app = Javalin.create().start(7000);

        // Map Routes
        setupCrud(app, "market-assets", MarketAsset.class);
        setupCrud(app, "data-sources", DataSource.class);

        app.post("/governance/grant", ctx -> {
            String cmd = ctx.queryParam("cmd");
            String result = executeGrant(cmd);
            ctx.result(result);
        });

        System.out.println(">>> Market Ops (Postgres) Live at port 7000");
    }

    // --- CRUD REPOSITORY LOGIC ---

    private static <T> void setupCrud(Javalin app, String path, Class<T> clazz) {
        String fullPath = "/catalog/" + path;

        // CREATE
        app.post(fullPath, ctx -> {
            if (!authorize(ctx, "ADMIN")) return;
            T item = ctx.bodyAsClass(clazz);
            
            // Logic for auto-populating history on creation
            if (item instanceof MarketAsset ma) {
                ma.history.add(new MarketAssetHistory(ma.currentPrice, ma.demand));
            }

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                session.merge(item); // merge handles both save and update
                tx.commit();
                ctx.status(201).json(item);
            }
        });

        // READ ALL
        app.get(fullPath, ctx -> {
            try (Session session = sessionFactory.openSession()) {
                List<T> results = session.createQuery("from " + clazz.getName(), clazz).list();
                ctx.json(results);
            }
        });

        // READ ONE
        app.get(fullPath + "/{id}", ctx -> {
            try (Session session = sessionFactory.openSession()) {
                T item = session.get(clazz, (Serializable) ctx.pathParam("id"));
                if (item != null) ctx.json(item); else ctx.status(404);
            }
        });
    }

    // --- GOVERNANCE & AUTH ---

    private static boolean authorize(Context ctx, String action) {
        String username = ctx.header("X-User");
        if (username == null) {
            ctx.status(403).result("Header X-User missing");
            return false;
        }

        try (Session session = sessionFactory.openSession()) {
            User user = session.get(User.class, username.toLowerCase());
            if (user != null && (user.permissions.contains(action + ":ALL"))) {
                return true;
            }
            ctx.status(403).result("Access Denied for " + username);
            return false;
        }
    }

    private static String executeGrant(String command) {
        // Simple Parser: GRANT ACTION ON TARGET TO USER
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
                return "SUCCESS: Granted " + action + " to " + username;
            }
        } catch (Exception e) {
            return "ERROR: Invalid syntax. Use 'GRANT READ ON ALL TO username'";
        }
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
