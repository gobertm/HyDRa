package be.unamur;

public interface DataInit {
    void initConnection();
    void persistData(int model, int numberofrecords);

    void deleteAll(String dbname);
}
