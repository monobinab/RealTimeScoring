package analytics.util.objects;

public class DivLn{
		public DivLn(String div, String ln) {
			this.div = div;
			this.ln = ln;
		}
		String div;
		String ln;
		public String getDiv(){
			return div;
		}
		public String getDivLn(){
			return ln;
		}

        public String toString()
        {
            return " div = "+ div + ", ln = "+ln;
        }

	
}
