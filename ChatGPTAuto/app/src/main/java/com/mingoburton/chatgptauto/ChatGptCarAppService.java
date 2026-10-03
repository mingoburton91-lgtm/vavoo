package com.mingoburton.chatgptauto;

import android.app.ActivityOptions;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.net.Uri;
import android.view.Display;

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

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";

    @NonNull
    @Override
    public HostValidator createHostValidator() {
        // This is a personal/debug sideload build, so allow the connected Android Auto host.
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
                    .addText("ChatGPT si apre sul telefono e l'audio continua attraverso l'auto.")
                    .build();

            Action start = new Action.Builder()
                    .setTitle("AVVIA VOCE")
                    .setOnClickListener(this::openChatGptOnPhone)
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

        private void openChatGptOnPhone() {
            CarContext context = getCarContext();

            Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse("https://chatgpt.com/"));
            intent.setPackage(CHATGPT_PACKAGE);
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);

            ActivityOptions options = ActivityOptions.makeBasic();
            options.setLaunchDisplayId(Display.DEFAULT_DISPLAY);

            try {
                context.startActivity(intent, options.toBundle());
                status = "ChatGPT avviato sul telefono. Puoi parlare.";
            } catch (ActivityNotFoundException e) {
                try {
                    Intent launch = context.getPackageManager().getLaunchIntentForPackage(CHATGPT_PACKAGE);
                    if (launch != null) {
                        launch.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
                        context.startActivity(launch, options.toBundle());
                        status = "ChatGPT avviato sul telefono. Puoi parlare.";
                    } else {
                        status = "ChatGPT non risulta installato sul telefono.";
                    }
                } catch (Exception ex) {
                    status = "Impossibile avviare ChatGPT sul telefono.";
                }
            } catch (Exception e) {
                status = "Android Auto ha bloccato l'avvio. Riprova a vettura ferma.";
            }

            invalidate();
        }
    }
}
