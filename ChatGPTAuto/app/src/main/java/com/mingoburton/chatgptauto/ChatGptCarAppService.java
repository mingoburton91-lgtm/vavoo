package com.mingoburton.chatgptauto;

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
                    .setTitle("ChatGPT Voce")
                    .addText(status)
                    .addText("Apre direttamente ChatGPT Voice sul telefono.")
                    .build();

            Action start = new Action.Builder()
                    .setTitle("AVVIA VOCE")
                    .setOnClickListener(this::launchBridge)
                    .build();

            Pane pane = new Pane.Builder()
                    .addRow(info)
                    .addAction(start)
                    .build();

            return new PaneTemplate.Builder(pane)
                    .setTitle("ChatGPT Auto")
                    .setHeaderAction(Action.APP_ICON)
                    .build();
        }

        private void launchBridge() {
            try {
                Intent bridge = new Intent(getCarContext(), VoiceBridgeActivity.class);
                bridge.addFlags(
                        Intent.FLAG_ACTIVITY_NEW_TASK |
                        Intent.FLAG_ACTIVITY_CLEAR_TOP
                );
                getCarContext().startActivity(bridge);
                status = "Avvio ChatGPT Voice sul telefono…";
            } catch (Exception e) {
                status = "Android Auto ha bloccato l'apertura sul telefono.";
            }
            invalidate();
        }
    }
}
