package c3g2.mrsort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GodsTrigger {
    public static void main(String[] args) throws IOException {
        Path cachePath = Paths.get("C:\\cache");
        List<String> cacheFiles = StreamSupport.stream(Files.newDirectoryStream(cachePath).spliterator(), false).map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
        for (char c = 'a'; c <= 'z'; c++) {
            char finalC = c;
            List<String> ofCat = cacheFiles.stream().filter(n -> n.startsWith(String.valueOf(finalC))).collect(Collectors.toList());
            if (ofCat.isEmpty()) {
                System.out.println("No files for category " + c);
                continue;
            }

            for (char s = 'a'; s <= 'z'; s++) {
                char finalS = s;
                ofCat.stream().filter(n -> n.startsWith(String.valueOf(finalC) + finalS))
                        .forEach(System.out::println);
            }
        }
    }
}
