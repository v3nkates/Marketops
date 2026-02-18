package marketops.catalog.repository;

import io.javalin.Javalin;
import io.javalin.http.Context;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class marketops2 {

    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    
    private static final GovernanceEngine engine = new GovernanceEngine();
    private static final String STORAGE_DIR = "./market_ops_store/";

    // --- MODELS ---
    public static class DataSource { public String id; public String name; public String type; public String format; public String connectionData; }
    public static class MarketAsset { public String id; public String name; public String type; public Double demand; public Double currentPrice; public List<MarketAssetHistory> history = new ArrayList<>(); }
    public static class MarketAssetHistory { public Double price; public Double demand; public long timestamp = System.currentTimeMillis(); public MarketAssetHistory() {} public MarketAssetHistory(Double p, Double d) { this.price = p; this.demand = d; } }
    public static class ETL { public String id; public String name; public String triggerType; public String dataSourceId; public String dataSetId; public String path; public String language; }
    public static class LineageTracking { public String id; public String dataSourceId; public String marketAssetId; public String userId; public String modelRegistryId; public long timestamp = System.currentTimeMillis(); }
    public static class LivePriceTracking { public String id; public String name; public String modelId; public String dataSourceId; public String simulationTrackingId; public Double toleranceRangeStart; public Double toleranceRangeEnd; }
    public static class DataSet { public String id; public String name; public String description; public String schema; public String path; }
    public static class ModelRegistry { public String id; public String modelName; public String description; public Map<String, Object> parameters; }
    public static class DistributionRegistry { public String id; public String factorName; public String signalType; public Map<String, Double> statistics; }
    public static class SimulationTracking { public String id; public String name; public String equation; public Map<String, String> distributionMapping; }
    public static class User { 
        public String username; 
        public Set<String> permissions = new HashSet<>(); 
        public User(String username) { this.username = username; } 
    }

    // --- GOVERNANCE ENGINE ---
    public static class GovernanceEngine {
        private final Map<String, User> userRegistry = new ConcurrentHashMap<>();
        private final Map<String, Object> objectStore = new ConcurrentHashMap<>();

        public void addUser(String name) { userRegistry.putIfAbsent(name.toLowerCase(), new User(name.toLowerCase())); }
        
        // Manual override for bootstrapping
        public void forceGrant(String username, String permission) {
            addUser(username);
            userRegistry.get(username.toLowerCase()).permissions.add(permission.toUpperCase());
        }

        public void registerObject(String id, Object obj) { if (id != null) objectStore.put(id.toLowerCase(), obj); }
        public Object getObject(String id) { return objectStore.get(id.toLowerCase()); }
        public List<Object> getAllByClass(Class<?> clazz) { return objectStore.values().stream().filter(clazz::isInstance).collect(Collectors.toList()); }

        public String executeGrant(String command) {
            try {
                String[] parts = command.split("\\s+");
                if (parts.length >= 6 && parts[0].equalsIgnoreCase("GRANT")) {
                    String action = parts[1].toUpperCase();
                    String username = parts[parts.length - 1].toLowerCase();
                    String target = parts[parts.length - 3].toUpperCase(); 

                    forceGrant(username, action + ":" + target);
                    return "SUCCESS: Granted " + action + " on " + target + " to " + username;
                }
            } catch (Exception e) { return "ERROR: Invalid syntax."; }
            return "ERROR: Parsing failure.";
        }

        public boolean hasAccess(String user, String action, String objId) {
            if (user == null) return false;
            User u = userRegistry.get(user.toLowerCase());
            if (u == null) return false;
            return u.permissions.contains(action.toUpperCase() + ":" + objId.toUpperCase()) || 
                   u.permissions.contains(action.toUpperCase() + ":ALL");
        }
        
        public Set<String> getPermissions(String user) {
            User u = userRegistry.get(user.toLowerCase());
            return (u != null) ? u.permissions : Collections.emptySet();
        }
    }

    public static void main(String[] args) {
        new File(STORAGE_DIR).mkdirs();
        
        // BOOTSTRAP: Direct injection to ensure admin_user works immediately
        engine.forceGrant("admin_user", "ADMIN:ALL");
        engine.addUser("data_scientist");

        Javalin app = Javalin.create(config -> { config.showJavalinBanner = false; }).start(7000);

        setupCrud(app, "market-assets", MarketAsset.class);
        setupCrud(app, "data-sources", DataSource.class);
        setupCrud(app, "data-sets", DataSet.class);
        setupCrud(app, "models", ModelRegistry.class);
        setupCrud(app, "distributions", DistributionRegistry.class);
        setupCrud(app, "simulations", SimulationTracking.class);

        app.post("/governance/grant", ctx -> { ctx.result(engine.executeGrant(ctx.queryParam("cmd"))); });
        
        System.out.println("\n>>> Market Ops Ready. Login with Header 'X-User: admin_user'");
    }

    private static <T> void setupCrud(Javalin app, String path, Class<T> clazz) {
        String fullPath = "/catalog/" + path;

        app.post(fullPath, ctx -> {
            if (!authorize(ctx, "ADMIN")) return;
            T item = ctx.bodyAsClass(clazz);
            String id = getEntityId(item);
            if (item instanceof MarketAsset) {
                MarketAsset ma = (MarketAsset) item;
                ma.history.add(new MarketAssetHistory(ma.currentPrice, ma.demand));
            }
            engine.registerObject(id, item);
            persist(id, item);
            ctx.status(201).json(item);
        });

        app.get(fullPath, ctx -> ctx.json(engine.getAllByClass(clazz)));
        app.get(fullPath + "/{id}", ctx -> {
            Object obj = engine.getObject(ctx.pathParam("id"));
            if (obj != null) ctx.json(obj); else ctx.status(404).result("Not Found");
        });
    }

    private static boolean authorize(Context ctx, String action) {
        String user = ctx.header("X-User");
        if (user != null && engine.hasAccess(user, action, "ALL")) return true;
        
        String perms = (user != null) ? engine.getPermissions(user).toString() : "No Header";
        ctx.status(403).result("Access Denied for user: " + user + ". Permissions: " + perms);
        return false;
    }

    private static void persist(String id, Object obj) { try { mapper.writeValue(new File(STORAGE_DIR + id + ".json"), obj); } catch (Exception e) {} }
    private static String getEntityId(Object obj) { try { return (String) obj.getClass().getField("id").get(obj); } catch (Exception e) { return UUID.randomUUID().toString(); } }
}