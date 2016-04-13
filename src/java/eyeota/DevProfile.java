package eyeota;
import java.util.HashSet;
import java.util.Set;
import java.io.Serializable;

public class DevProfile implements Serializable {
        private String id;
        private String segs;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getSegset() {
            return segs;
        }

        public void setSegset(String ss) {
            this.segs = ss;
        }
}
