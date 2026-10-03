package com.mingoburton.chatgptauto;

import android.app.PendingIntent;
import android.content.Intent;

import androidx.annotation.NonNull;
import androidx.car.app.CarAppService;
import androidx.car.app.CarContext;
import androidx.car.app.Screen;
import androidx.car.app.Session;
import androidx.car.app.model.Action;
import androidx.car.app.model.Pane;
import androidx.car.app.model.PaneTemplate;
import androidx.car.app.model.Row;
import androidx.car.app.model.Template;
import androidx.car.app.validation.HostValidator;

public class ChatGptCarAppService extends CarAppService {

    @NonNull
    @Override
    public HostValidator createHostValidator() {
        return HostValidator.ALLOW_ALL_HOSTS_VALIDATOR;
    }

    @NonNull
    @Override
    public Session onCreateSession() {
        return new Session() {
            @NonNull
            @Override
            public Screen onCreateScreen(@NonNull Intent intent) {
                return new ChatGptScreen(getCarContext());
            }
        };
    }

    private static class ChatGptScreen extends Screen {
        private String status = "Pronto. Premi AVVIA VOCE.";

        ChatGptScreen(@NonNull CarContext carContext) {
            super(carContext);
        }

        @NonNull
        @Override
        public Template onGetTemplate() {
            Row info = new Row.Builder()
                    .setTitle("ChatGPT Voce · v1.4")
                    .addText(status)
                    .addText("Apre direttamente l'app ChatGPT. Nessun collegamento web.")
                    .build();

            Action voice = new Action.Builder()
                    .setTitle("AVVIA VOCE")
                    .setOnClickListener(this::launchPhoneTrampoline)
                    .build();

            Pane pane = new Pane.Builder()
                    .addRow(info)
                    .addAction(voice)
                    .build();

            return new PaneTemplate.Builder(pane)
                    .setTitle("ChatGPT Auto · v1.4")
                    .setHeaderAction(Action.APP_ICON)
                    .build();
        }

        private void launchPhoneTrampoline() {
            CarContext context = getCarContext();

            Intent phoneIntent = new Intent(context, VoiceLaunchActivity.class);
            phoneIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
            phoneIntent.putExtra("from_car", true);

            PendingIntent pendingIntent = PendingIntent.getActivity(
                    context,
                    140,
                    phoneIntent,
                    PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
            );

            try {
                pendingIntent.send();
                status = "Richiesta inviata al telefono.";
            } catch (PendingIntent.CanceledException e) {
                status = "Avvio sul telefono non riuscito.";
            }

            invalidate();
        }
    }
}
