import java.util.Scanner;
//main class to be run to start Davisql
public class DavisBasePrompt {

    private static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {

        DatabaseUtility.initializeTheDatabase();
        splashScreen();

        while(!QueryParser.isExit) {
            System.out.print(DatabaseDefinedConstants.PROMPT);
            String command = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            QueryParser.parseCommand(command);
        }
    }

    private static void splashScreen() {
        System.out.println(QueryHandling.line("-",80));
        System.out.println("Welcome to DavisBase"); // Display the string.
        QueryHandling.ShowVersionQueryHandler();
        System.out.println("\nType 'help;' to display supported commands.");
        System.out.println(QueryHandling.line("-",80));
    }
}