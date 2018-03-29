import map.DistributedHashMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Scanner;

public class ManagementTool {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    DistributedHashMap map;

    public ManagementTool() throws Exception {
        map = new DistributedHashMap();
    }

    public void start() throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        String line, key, value;
        try {
            while (!(line = scanner.nextLine()).toUpperCase().equals("EXIT")) {
                switch (line.toUpperCase().trim()) {
                    case "PUT":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        System.out.print("Value:\t");
                        value = scanner.nextLine();
                        map.put(key, value);
                        break;
                    case "GET":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        map.get(key);
                        break;
                    case "REMOVE":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        map.remove(key);
                        break;
                    case "CONTAINS":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        map.containsKey(key);
                        break;
                    case "STATE":
                        map.printState();
                        break;
                    case "CLEAR":
                        map.clear();
                        break;
                    default:
                        System.out.println("Not a valid operation, possible are: put, get, remove, contains, state, clear");
                }
            }
        } finally {
            map.close();
        }
    }
}
