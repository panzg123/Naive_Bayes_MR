package cn.edu.ucas.bean;

public class CategoryTotal implements Comparable<CategoryTotal>{
	private String word;
	private String category;
	private double times;
	private double categoryFileTotal;
	private double totalFile;
	
	/**
	 * @return the word
	 */
	public String getWord() {
		return word;
	}
	/**
	 * @param word the word to set
	 */
	public void setWord(String word) {
		this.word = word;
	}
	/**
	 * @return the category
	 */
	public String getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(String category) {
		this.category = category;
	}
	/**
	 * @return the times
	 */
	public double getTimes() {
		return times;
	}
	/**
	 * @param times the times to set
	 */
	public void setTimes(double times) {
		this.times = times;
	}
	/**
	 * @return the categoryFileTotal
	 */
	public double getCategoryFileTotal() {
		return categoryFileTotal;
	}
	/**
	 * @param categoryFileTotal the categoryFileTotal to set
	 */
	public void setCategoryFileTotal(double categoryFileTotal) {
		this.categoryFileTotal = categoryFileTotal;
	}
	/**
	 * @return the totalFile
	 */
	public double getTotalFile() {
		return totalFile;
	}
	/**
	 * @param totalFile the totalFile to set
	 */
	public void setTotalFile(double totalFile) {
		this.totalFile = totalFile;
	}
	public CategoryTotal() {
		super();
	}
	public CategoryTotal(String word, String category, double times, double categoryFileTotal, double totalFile) {
		super();
		this.word = word;
		this.category = category;
		this.times = times;
		this.categoryFileTotal = categoryFileTotal;
		this.totalFile = totalFile;
	}
	public CategoryTotal(String category, double categoryFileTotal, double totalFile) {
		super();
		this.category = category;
		this.categoryFileTotal = categoryFileTotal;
		this.totalFile = totalFile;
	}
	@Override
	public int compareTo(CategoryTotal o) {
		int out = -2;
		if(this.word.equals(o.getWord())){
			out = 0;
		}
		if(this.category.equals(o.getCategory())){
			out = 0;
		}
		if(this.times==o.getTimes()){
			out = 0;
		}
		if(this.categoryFileTotal==o.getCategoryFileTotal()){
			out = 0;
		}
		if(this.totalFile==o.getTotalFile()){
			out = 0;
		}
		return out;
	}
}
