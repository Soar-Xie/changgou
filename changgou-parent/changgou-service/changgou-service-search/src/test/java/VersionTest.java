import org.junit.Test;
import org.springframework.boot.SpringBootVersion;
import org.springframework.core.SpringVersion;

public class VersionTest {

    @Test
    public void testVersion() {
        System.out.println(SpringVersion.getVersion());
        System.out.println(SpringBootVersion.getVersion());
    }
}
