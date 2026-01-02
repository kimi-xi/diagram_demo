```mermaid
sequenceDiagram
    autonumber
    actor Nutzer as Nutzer
    participant Expo as Expo App (React Native)
    participant FEAuth as useAuthController
    participant ApiRepo as ApiAuthRepository
    participant Express as Express Router (/auth)
    participant Prisma as Prisma ORM
    participant PG as PostgreSQL (HabitTrackerDatabase)

    Nutzer->>Expo: E-Mail & Passwort eingeben
    Expo->>FEAuth: login(email, password)
    FEAuth->>ApiRepo: POST /auth/login

    ApiRepo->>Express: HTTP Request (email, password)
    Express->>Prisma: prisma.user.findUnique({ where: { email } })
    Prisma->>PG: SELECT * FROM "User" WHERE email = ...
    PG-->>Prisma: UserRecord
    Prisma-->>Express: UserRecord

    alt User nicht gefunden
        Express-->>ApiRepo: 401 Unauthorized
        ApiRepo-->>FEAuth: Fehler
        FEAuth-->>Expo: Fehlermeldung anzeigen
    else User gefunden
        Express->>Express: Passwortvergleich (bcrypt.compare)
        alt Passwort korrekt
            Express->>Express: JWT Token erzeugen
            Express-->>ApiRepo: 200 OK (token, user)
            ApiRepo-->>FEAuth: token + user
            FEAuth-->>Expo: Login erfolgreich
        else Passwort falsch
            Express-->>ApiRepo: 401 Unauthorized
            ApiRepo-->>FEAuth: Fehler
            FEAuth-->>Expo: Fehlermeldung anzeigen
        end
    end
