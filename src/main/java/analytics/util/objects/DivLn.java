package analytics.util.objects;

public class DivLn{
		public DivLn(String div, String ln, String tag) {
			this.div = div;
			this.ln = ln;
			this.tag = tag;
		}
		
		public DivLn(String div, String ln) {
			this.div = div;
			this.ln = ln;
		}
				
		String div;
		String ln;
		String tag;
		public String getTag() {
			return tag;
		}

		public String getDiv(){
			return div;
		}
		public String getDivLn(){
			return ln;
		}

        public String toString()
        {
            return " div = "+ div + ", ln = "+ln + ", tag " + tag;
        }

	}
