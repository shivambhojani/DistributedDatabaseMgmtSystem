package Analytics;

public class QueriesCounter {

    private int numberOfQueries;
    private String vmName;
    private String userName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getNumberOfQueries() {
        return numberOfQueries;
    }

    public void setNumberOfQueries(int numberOfQueries) {
        this.numberOfQueries = numberOfQueries;
    }

    public String getVmName() {
        return vmName;
    }

    public void setVmName(String vmName) {
        this.vmName = vmName;
    }

}
