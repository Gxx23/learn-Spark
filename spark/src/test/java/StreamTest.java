import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StreamTest {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        list.forEach(System.out::println);
        List<Integer> list1 = list.stream()
                .filter(n -> n % 2 == 0)
                .map(n -> n * n).collect(Collectors.toList());
        list1.forEach(System.out::println);
        list.forEach(System.out::println);
    }
}
